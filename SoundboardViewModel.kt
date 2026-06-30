package com.voxasoundboard.app.ui.features.soundboard

import android.net.Uri
import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import com.google.firebase.crashlytics.FirebaseCrashlytics
import com.voxasoundboard.app.FreeTierLimits
import com.voxasoundboard.app.analytics.Analytics
import com.voxasoundboard.app.analytics.AnalyticsTracker
import com.voxasoundboard.app.audio.DeleteSoundFromSoundboardUseCase
import com.voxasoundboard.app.audio.ImportAudioUseCase
import com.voxasoundboard.app.audio.SoundKey
import com.voxasoundboard.app.audio.SoundPlayer
import com.voxasoundboard.app.data.db.entities.GeneralUiSettings
import com.voxasoundboard.app.data.db.entities.MultipleSoundsSelected
import com.voxasoundboard.app.data.db.entities.PerSoundPlaybackSettings
import com.voxasoundboard.app.data.db.entities.PlaybackMode
import com.voxasoundboard.app.data.db.entities.Sound
import com.voxasoundboard.app.data.db.relations.SoundboardWithSounds
import com.voxasoundboard.app.data.repositories.GeneralUiSettingsRepository
import com.voxasoundboard.app.data.repositories.GlobalPlaybackSettingsRepository
import com.voxasoundboard.app.data.repositories.OnboardingRepository
import com.voxasoundboard.app.data.repositories.PerSoundPlaybackSettingsRepository
import com.voxasoundboard.app.data.repositories.ProRepository
import com.voxasoundboard.app.data.repositories.SoundboardRepository
import com.voxasoundboard.app.sync.RestoreAudioUseCase
import com.voxasoundboard.app.sync.RestoreResult
import com.voxasoundboard.app.ui.features.soundboard.model.MapSoundsToUiStates
import com.voxasoundboard.app.ui.features.soundboard.model.PadPlaybackState
import com.voxasoundboard.app.ui.features.soundboard.model.PadRuntimeState
import com.voxasoundboard.app.ui.features.soundboard.model.SoundWithUiState
import com.voxasoundboard.app.ui.features.soundboard.model.SoundboardScreenUserMessage
import com.voxasoundboard.app.ui.models.ProTier
import dagger.hilt.android.lifecycle.HiltViewModel
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.SharingStarted
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.combine
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.stateIn
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import java.io.File
import javax.inject.Inject

@HiltViewModel
class SoundboardViewModel @Inject constructor(
    private val analyticsTracker: AnalyticsTracker,
    private val soundboardRepo: SoundboardRepository,
    private val globalPlaybackSettingsRepo: GlobalPlaybackSettingsRepository,
    private val perSoundPlaybackSettingsRepo: PerSoundPlaybackSettingsRepository,
    generalUiSettingsRepo: GeneralUiSettingsRepository,
    proRepo: ProRepository,
    private val importAudioUseCase: ImportAudioUseCase,
    private val deleteSoundFromSoundboard: DeleteSoundFromSoundboardUseCase,
    private val soundPlayer: SoundPlayer,
    private val mapSoundsToUiStates: MapSoundsToUiStates,
    private val onboardingRepo: OnboardingRepository,
    private val restoreAudioUseCase: RestoreAudioUseCase
) : ViewModel() {
    private val _isReorderMode = MutableStateFlow(false)
    val isReorderMode: StateFlow<Boolean> = _isReorderMode.asStateFlow()

    private val _isBulkDeleteMode = MutableStateFlow(false)
    val isBulkDeleteMode: StateFlow<Boolean> = _isBulkDeleteMode.asStateFlow()

    private val _pendingDeleteIds = MutableStateFlow<Set<Long>>(emptySet())
    val pendingDeleteIds: StateFlow<Set<Long>> = _pendingDeleteIds.asStateFlow()

    private val _baseSoundsWithUiStates = MutableStateFlow<List<SoundWithUiState>>(emptyList())
    private val _dragState = MutableStateFlow<List<SoundWithUiState>?>(null)
    val soundsWithUiStates: StateFlow<List<SoundWithUiState>> =
        combine(_baseSoundsWithUiStates, _dragState) { base, drag ->
            drag ?: base
        }.stateIn(
            scope = viewModelScope,
            started = SharingStarted.WhileSubscribed(5_000),
            initialValue = emptyList()
        )

    val generalUiSettings: StateFlow<GeneralUiSettings> = generalUiSettingsRepo.observeSettings()
        .stateIn(
            scope = viewModelScope,
            started = SharingStarted.WhileSubscribed(5_000),
            initialValue = GeneralUiSettings()
        )

    val isPro: StateFlow<Boolean> = proRepo.proSettings
        .map { it.tier == ProTier.PRO }
        .stateIn(
            scope = viewModelScope,
            started = SharingStarted.Eagerly,
            initialValue = false
        )

    val showEditHint: StateFlow<Boolean> = onboardingRepo.shouldShowEditHint
        .stateIn(
            scope = viewModelScope,
            started = SharingStarted.WhileSubscribed(5_000),
            initialValue = false
        )

    private val _showAddSoundHint = MutableStateFlow(false)
    val showAddSoundHint: StateFlow<Boolean> = _showAddSoundHint.asStateFlow()

    private val _showMessageToTheUser = MutableSharedFlow<SoundboardScreenUserMessage>(extraBufferCapacity = 1)
    val showMessageToTheUser: SharedFlow<SoundboardScreenUserMessage> = _showMessageToTheUser.asSharedFlow()

    private val _bulkDeleteCompleted = MutableSharedFlow<Int>(extraBufferCapacity = 1)
    val bulkDeleteCompleted: SharedFlow<Int> = _bulkDeleteCompleted.asSharedFlow()

    private val _isRestoring = MutableStateFlow(false)
    val isRestoring: StateFlow<Boolean> = _isRestoring.asStateFlow()

    private val _isImporting = MutableStateFlow(false)
    val isImporting: StateFlow<Boolean> = _isImporting.asStateFlow()

    private val _restoreResult = MutableSharedFlow<RestoreResult>(extraBufferCapacity = 1)
    val restoreResult: SharedFlow<RestoreResult> = _restoreResult.asSharedFlow()

    private var soundboardId: Long = -1
    private var soundboardCollectorJob: Job? = null
    private var settingsObserverJob: Job? = null
    private var playbackErrorJob: Job? = null

    fun dismissEditHint() {
        viewModelScope.launch { onboardingRepo.markEditHintSeen() }
    }

    fun dismissAddSoundHint() {
        viewModelScope.launch {
            onboardingRepo.markAddSoundHintSeen()
            _showAddSoundHint.value = false
        }
    }

    fun stopAll() {
        soundPlayer.stopAllForSoundboard(soundboardId, 500L)
    }

    fun restoreSounds() {
        if (_isRestoring.value) return
        viewModelScope.launch {
            _isRestoring.value = true
            try {
                val result = restoreAudioUseCase()
                _restoreResult.tryEmit(result)
                // fileExistsCache is recomputed automatically when updateSoundFileName
                // triggers the soundboard Room flow to re-emit.
            } finally {
                _isRestoring.value = false
            }
        }
    }

    fun sortSoundsByNameAscending() {
        viewModelScope.launch {
            soundboardRepo.sortSoundsByNameAscending(soundboardId)
        }
    }

    fun setSoundboard(id: Long) {
        soundboardId = id
        soundboardCollectorJob?.cancel()
        settingsObserverJob?.cancel()
        playbackErrorJob?.cancel()
        playbackErrorJob = viewModelScope.launch {
            soundPlayer.playbackErrors.collect { soundKey ->
                if (soundKey.soundboardId == soundboardId) {
                    _showMessageToTheUser.tryEmit(SoundboardScreenUserMessage.PLAYBACK_FAILED)
                }
            }
        }

        settingsObserverJob = viewModelScope.launch {
            combine(
                globalPlaybackSettingsRepo.observeSettings(),
                perSoundPlaybackSettingsRepo.observeAllForSoundboard(soundboardId),
                soundPlayer.playbackStates
            ) { global, perSoundList, playbackStates ->
                val perSoundMap = perSoundList.associateBy { it.soundId }
                Triple(global, perSoundMap, playbackStates)
            }.collect { (global, perSoundMap, playbackStates) ->
                playbackStates.keys
                    .filter { it.soundboardId == soundboardId }
                    .forEach { soundKey ->
                        val perSound = perSoundMap[soundKey.soundId]
                        val loop = when (perSound?.playbackMode ?: global.playbackMode) {
                            PlaybackMode.LOOP -> true
                            PlaybackMode.ONE_SHOT -> false
                        }
                        val volume = ((perSound?.volume ?: global.volume) / 100f).coerceIn(0f, 1f)
                        val fadeOutMs = perSound?.fadeOutDurationMs ?: global.fadeOutDurationMs
                        soundPlayer.updatePlayback(soundKey, loop, volume, fadeOutMs)
                    }
            }
        }

        soundboardCollectorJob = viewModelScope.launch {
            var cachedOrderedIds: List<Long> = emptyList()
            var fileExistsCache: Map<Long, Boolean> = emptyMap()

            soundboardRepo.getSoundboard(soundboardId)
                .onEach { soundboard ->
                    try {
                        cachedOrderedIds = soundboardRepo.getSoundIdsInOrder(soundboardId)
                        fileExistsCache = withContext(Dispatchers.IO) {
                            soundboard?.sounds.orEmpty().associate { it.id to File(it.fileName).exists() }
                        }
                    } catch (e: CancellationException) {
                        throw e
                    } catch (e: Exception) {
                        FirebaseCrashlytics.getInstance().recordException(e)
                    }
                }
                .combine(soundPlayer.playbackStates) { soundboard, playbackStates ->
                    soundboard to playbackStates
                }
                .collect { (soundboard, playbackStates) ->
                    val sounds = soundboard?.sounds.orEmpty()

                    val soundsWithPadRuntimeState = hashMapOf<Sound, PadRuntimeState>()
                    sounds.forEach { sound ->
                        val playbackState = playbackStates[SoundKey(soundboardId, sound.id)]
                        soundsWithPadRuntimeState[sound] = PadRuntimeState(
                            playbackState = when {
                                playbackState == null || !playbackState.isPlaying -> PadPlaybackState.IDLE
                                playbackState.isLooping -> PadPlaybackState.PLAYING_LOOPING
                                else -> PadPlaybackState.PLAYING_ONE_SHOT
                            },
                            queueNumber = playbackState?.queueNumber ?: -1,
                            positionMs = playbackState?.positionMs ?: 0L,
                            durationMs = playbackState?.durationMs ?: 0L,
                            progress = playbackState?.progress ?: 0f,
                        )
                    }

                    val mapped = mapSoundsToUiStates.map(soundsWithPadRuntimeState, soundboardId)
                        .map { it.copy(fileExists = fileExistsCache[it.sound.id] ?: true) }
                    _baseSoundsWithUiStates.value = if (cachedOrderedIds.isNotEmpty()) {
                        mapped.sortedBy { cachedOrderedIds.indexOf(it.sound.id) }
                    } else {
                        mapped
                    }
                }
        }
    }

    fun getSoundboardWithSounds(id: Long): Flow<SoundboardWithSounds?> =
        soundboardRepo.getSoundboard(id)

    fun onAddSoundPadClicked() {
        viewModelScope.launch {
            val currentSoundCount = soundboardRepo.getSoundboard(soundboardId).first()?.sounds?.size ?: 0
            if (!isPro.value && currentSoundCount >= FreeTierLimits.FREE_SOUND_LIMIT) {
                _showMessageToTheUser.tryEmit(SoundboardScreenUserMessage.SOUND_LIMIT_REACHED)
                analyticsTracker.logEvent(Analytics.EVENT_PRO_PAYWALL_SHOWN, mapOf(Analytics.PARAM_TRIGGER to Analytics.TRIGGER_ADD_SOUND_PAD))
            } else {
                _showMessageToTheUser.tryEmit(SoundboardScreenUserMessage.LAUNCH_FILE_PICKER)
            }
        }
    }

    fun onRecordFromMicClicked() {
        viewModelScope.launch {
            val currentSoundCount = soundboardRepo.getSoundboard(soundboardId).first()?.sounds?.size ?: 0
            if (!isPro.value && currentSoundCount >= FreeTierLimits.FREE_SOUND_LIMIT) {
                _showMessageToTheUser.tryEmit(SoundboardScreenUserMessage.SOUND_LIMIT_REACHED)
                analyticsTracker.logEvent(Analytics.EVENT_PRO_PAYWALL_SHOWN, mapOf(Analytics.PARAM_TRIGGER to Analytics.TRIGGER_RECORD_FROM_MIC))
            } else {
                _showMessageToTheUser.tryEmit(SoundboardScreenUserMessage.NAVIGATE_TO_MIC_RECORD)
            }
        }
    }

    fun onAddSoundsSelected(uris: List<Uri>) {
        if (uris.isEmpty()) return
        if (_isImporting.value) return
        viewModelScope.launch {
            val currentSoundCount = soundboardRepo.getSoundboard(soundboardId).first()?.sounds?.size ?: 0
            val slotsRemaining = if (isPro.value) Int.MAX_VALUE
                else FreeTierLimits.FREE_SOUND_LIMIT - currentSoundCount
            if (slotsRemaining <= 0) {
                _showMessageToTheUser.tryEmit(SoundboardScreenUserMessage.SOUND_LIMIT_REACHED)
                analyticsTracker.logEvent(Analytics.EVENT_PRO_PAYWALL_SHOWN, mapOf(Analytics.PARAM_TRIGGER to Analytics.TRIGGER_IMPORT_SOUNDS))
                return@launch
            }
            val urisToImport = uris.take(slotsRemaining)
            val limitWillBeReached = urisToImport.size < uris.size
            _isImporting.value = true
            var failureCount = 0
            try {
                for (uri in urisToImport) {
                    try {
                        importAudioUseCase(sourceUri = uri, soundboardId = soundboardId)
                    } catch (e: CancellationException) {
                        throw e
                    } catch (e: Exception) {
                        FirebaseCrashlytics.getInstance().recordException(e)
                        failureCount++
                    }
                }
            } finally {
                _isImporting.value = false
            }
            if (failureCount > 0) {
                _showMessageToTheUser.tryEmit(SoundboardScreenUserMessage.IMPORT_FAILED)
            }
            if (limitWillBeReached) {
                _showMessageToTheUser.tryEmit(SoundboardScreenUserMessage.SOUND_LIMIT_REACHED)
            }
            val successCount = urisToImport.size - failureCount
            if (successCount == 1 && onboardingRepo.shouldShowAddSoundHint.first()) {
                _showAddSoundHint.value = true
            }
        }
    }

    fun onPadPressed(sound: Sound) {
        viewModelScope.launch {
            onboardingRepo.recordPlay()
            val globalSoundSettings = globalPlaybackSettingsRepo.getSettings()
            val perSoundSettings = perSoundPlaybackSettingsRepo.getSettings(
                soundId = sound.id,
                soundboardId = soundboardId
            ) ?: PerSoundPlaybackSettings(soundId = sound.id, soundboardId = soundboardId)

            soundPlayer.playOrQueueSound(
                soundboardId = soundboardId,
                sound = sound,
                loop = when (perSoundSettings.playbackMode) {
                    PlaybackMode.LOOP -> true
                    PlaybackMode.ONE_SHOT -> false
                    null -> globalSoundSettings.playbackMode == PlaybackMode.LOOP
                },
                volume = (perSoundSettings.volume ?: globalSoundSettings.volume) / 100f,
                fadeInMs = perSoundSettings.fadeInDurationMs ?: globalSoundSettings.fadeInDurationMs,
                fadeOutMs = perSoundSettings.fadeOutDurationMs ?: globalSoundSettings.fadeOutDurationMs,
                allowOverlap = globalSoundSettings.multipleSoundsSelected == MultipleSoundsSelected.OVERLAP,
                tapAction = globalSoundSettings.tapAction
            )
        }
    }

    fun toggleReorderMode() {
        if (!_isReorderMode.value && soundPlayer.isAnySoundPlayingCurrently()) {
            _showMessageToTheUser.tryEmit(SoundboardScreenUserMessage.STOP_PLAYBACK_BEFORE_REORDERING)
        } else {
            _isReorderMode.value = !_isReorderMode.value
        }
    }

    fun stopAllPlaybackAndReorderSounds() {
        soundPlayer.stopAllForSoundboard(soundboardId)
        _isReorderMode.value = true
    }

    fun cancelMove() {
        if (soundboardId == -1L) return
        _dragState.value = null
        toggleReorderMode()
    }

    fun toggleBulkDeleteMode() {
        if (!_isBulkDeleteMode.value) {
            if (soundPlayer.isAnySoundPlayingCurrently()) {
                _showMessageToTheUser.tryEmit(SoundboardScreenUserMessage.STOP_PLAYBACK_BEFORE_BULK_DELETE)
                return
            }
            _pendingDeleteIds.value = emptySet()
        }
        _isBulkDeleteMode.value = !_isBulkDeleteMode.value
    }

    fun toggleSoundForDeletion(soundId: Long) {
        val current = _pendingDeleteIds.value.toMutableSet()
        if (soundId in current) current.remove(soundId) else current.add(soundId)
        _pendingDeleteIds.value = current
    }

    fun confirmBulkDelete() {
        val ids = _pendingDeleteIds.value.toList()
        viewModelScope.launch {
            try {
                ids.forEach { soundId ->
                    deleteSoundFromSoundboard(
                        soundId = soundId,
                        soundboardId = soundboardId
                    )
                }
                _pendingDeleteIds.value = emptySet()
                _isBulkDeleteMode.value = false
                _bulkDeleteCompleted.emit(ids.size)
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                FirebaseCrashlytics.getInstance().recordException(e)
                _pendingDeleteIds.value = emptySet()
                _isBulkDeleteMode.value = false
                _showMessageToTheUser.tryEmit(SoundboardScreenUserMessage.DELETE_FAILED)
            }
        }
    }

    fun cancelBulkDelete() {
        _pendingDeleteIds.value = emptySet()
        _isBulkDeleteMode.value = false
    }

    fun moveSound(fromIndex: Int, toIndex: Int) {
        if (fromIndex == toIndex) return
        val current = (_dragState.value ?: _baseSoundsWithUiStates.value).toMutableList()
        if (fromIndex !in current.indices || toIndex !in current.indices) return
        val moved = current.removeAt(fromIndex)
        current.add(toIndex, moved)
        _dragState.value = current
    }

    fun persistSoundOrder() {
        if (soundboardId == -1L) return
        viewModelScope.launch {
            soundboardRepo.updateSoundPositions(
                soundboardId = soundboardId,
                soundIdsInOrder = _dragState.value?.map { it.sound.id } ?: emptyList()
            )
            _dragState.value = null
        }
    }
}
