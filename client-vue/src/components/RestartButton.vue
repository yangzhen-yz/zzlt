<template>
  <!-- 放在全局顶栏：首屏就加载好了，页面卡顿时不用等设置页的懒加载 chunk -->
  <div class="restart-wrap">
    <button
      class="restart-btn"
      :class="{ pending: pending, busy: busy }"
      :disabled="busy || pending || !canRestart"
      :title="title"
      @click="onClick"
    >
      <span class="ico">⟳</span>
      <span class="label">{{ label }}</span>
      <span v-if="pending" class="dot" aria-hidden="true"></span>
    </button>
  </div>
</template>

<script setup>
  import { ref, computed, inject, onMounted, onUnmounted } from 'vue'
  import { apiFetch, getToken } from '../utils/helpers'

  const showNotification = inject('showNotification', () => {})

  const canRestart = ref(true)
  const pending = ref(false) // 已提交、看护还没消费
  const busy = ref(false) // 提交中 / 等新进程起来
  const deferred = ref(false)
  const note = ref('')

  let pollTimer = null
  let pollLeft = 0
  let baselineBootId = ''

  const POLL_INTERVAL_MS = 5000
  const POLL_MAX = 180 // 15 分钟
  const STATUS_URL = '/api/system/restart-status'

  const label = computed(() => {
    if (busy.value) return '重启中…'
    if (pending.value) return deferred.value ? '盘后重启' : '待重启'
    return '重启服务'
  })

  const title = computed(() => {
    if (!canRestart.value) return '只有管理员账号能重启服务'
    if (pending.value) return note.value || '已有待处理的重启请求，请勿重复提交'
    return '页面卡顿、数据不刷新时，让看护重启后端（约 5 分钟内生效）'
  })

  async function refreshStatus() {
    try {
      const res = await apiFetch(STATUS_URL, { timeout: 5000 })
      const d = (res && res.data) || {}
      if (typeof d.canRestart === 'boolean') canRestart.value = d.canRestart
      pending.value = !!d.pending
      deferred.value = !!d.deferred
      note.value = d.note || ''
      if (d.bootId) baselineBootId = d.bootId
    } catch (e) {
      // 卡顿时状态接口也会超时：静默失败，按钮仍可用，提交时由 POST 给真实反馈。
    }
  }

  function stopPolling() {
    if (pollTimer) clearTimeout(pollTimer)
    pollTimer = null
  }

  // 重启期间请求必然失败，用原生 fetch 静默重试：不走 apiFetch，避免刷「服务离线」通知。
  // bootId 变了才说明新进程真的起来了（只等「请求成功」会把旧进程误判成已恢复）。
  async function pollOnce() {
    const ctrl = new AbortController()
    const timer = setTimeout(() => ctrl.abort(), 4000)
    try {
      const token = getToken()
      const resp = await fetch(STATUS_URL + '?_=' + Date.now(), {
        headers: token ? { Authorization: `Bearer ${token}` } : {},
        cache: 'no-store',
        signal: ctrl.signal,
      })
      if (resp.ok) {
        const payload = await resp.json()
        const d = (payload && payload.data) || {}
        if (d.bootId && baselineBootId && d.bootId !== baselineBootId) {
          stopPolling()
          pending.value = false
          busy.value = false
          deferred.value = false
          showNotification('服务已重启完成，页面恢复可用', 'success')
          refreshStatus()
          return
        }
      }
    } catch (e) {
      // 断连是预期内的，静默等下一轮
    } finally {
      clearTimeout(timer)
    }
    if (--pollLeft > 0) {
      pollTimer = setTimeout(pollOnce, POLL_INTERVAL_MS)
    } else {
      stopPolling()
      busy.value = false
      showNotification('15 分钟内没检测到新进程，请检查看护 GupiaoBackendWatchdog 是否在运行', 'error')
    }
  }

  function startPolling() {
    stopPolling()
    pollLeft = POLL_MAX
    pollTimer = setTimeout(pollOnce, POLL_INTERVAL_MS)
  }

  async function onClick() {
    if (pending.value || busy.value) {
      showNotification(note.value || '已有待处理的重启请求，请勿重复提交', 'info')
      return
    }
    const ok = window.confirm(
      '确认重启后端服务？\n\n' +
        '· 提交后由 SYSTEM 看护在 5 分钟内执行，重启窗口约 1～3 分钟页面不可用；\n' +
        '· 工作日 09:10～15:10 会保留请求并推迟到 15:10 之后，避免打断推荐批次；\n' +
        '· 只重启后端进程，不改配置、不动数据；重启完成会发企微通知。\n\n' +
        '若服务已卡到完全不应答，这个请求同样发不出去——那种情况由自愈线程' +
        '（连续 3 次探活失败）自动写标志接管。'
    )
    if (!ok) return
    busy.value = true
    try {
      const res = await apiFetch('/api/system/restart' +
        '?reason=' + encodeURIComponent('页面主动重启'), { method: 'POST', timeout: 10000 })
      // apiFetch 对 4xx 是 resolve 一个 {detail}，不会抛异常，这里要自己判
      if (!res || res.success === false || res.detail) {
        throw new Error((res && res.detail) || '提交失败')
      }
      const d = res.data || {}
      if (d.bootId) baselineBootId = d.bootId
      pending.value = true
      deferred.value = !!d.deferred
      note.value = d.note || ''
      if (d.deferred) {
        busy.value = false
        showNotification('已提交：当前在盘中窗口，看护会推迟到 15:10 之后重启，完成后发企微通知', 'info')
        return
      }
      if (d.already) {
        showNotification('已有一份待处理的重启请求，本按钮会继续跟踪它的状态', 'info')
      } else {
        showNotification('重启请求已提交，看护将在 5 分钟内执行；期间页面可能短暂不可用', 'info')
      }
      startPolling()
    } catch (e) {
      busy.value = false
      const msg = e && e.message ? e.message : String(e)
      // 区分「没权限/参数错」与「服务卡到不应答」，后者要如实说明自愈接管
      if (/管理员|403|未登录|401/.test(msg)) {
        showNotification(msg, 'error')
      } else {
        showNotification(
          '服务当前无响应，重启请求未送达；若持续无响应，系统自愈线程会在连续探活失败后自动接管',
          'error'
        )
      }
    }
  }

  onMounted(() => {
    refreshStatus().then(() => {
      // 页面打开时就已有待处理请求（别人提交的 / 自愈写的），跟着它一起跟踪恢复
      if (pending.value && !deferred.value && baselineBootId) startPolling()
    })
  })
  onUnmounted(stopPolling)
</script>

<style scoped>
  .restart-wrap {
    flex: 0 0 auto;
    display: flex;
    align-items: center;
    padding-right: var(--content-padding);
  }
  .restart-btn {
    display: inline-flex;
    align-items: center;
    gap: 5px;
    padding: 6px 10px;
    border: 1px solid var(--border-medium);
    border-radius: var(--radius-sm);
    background: transparent;
    color: var(--text-secondary);
    font-family: inherit;
    font-size: 12px;
    line-height: 1.2;
    white-space: nowrap;
    cursor: pointer;
    transition: color var(--transition-fast), border-color var(--transition-fast);
  }
  .restart-btn:hover:not(:disabled) {
    color: var(--accent-danger);
    border-color: var(--accent-danger);
  }
  .restart-btn:disabled {
    opacity: 0.55;
    cursor: not-allowed;
  }
  .restart-btn .ico {
    font-size: 14px;
    line-height: 1;
  }
  .restart-btn.busy .ico {
    animation: restart-spin 1.6s linear infinite;
  }
  .dot {
    width: 6px;
    height: 6px;
    border-radius: 50%;
    background: var(--accent-warning);
  }
  @keyframes restart-spin {
    to {
      transform: rotate(360deg);
    }
  }
  @media (max-width: 600px) {
    .restart-wrap {
      padding-right: var(--spacing-sm);
    }
    .restart-btn {
      padding: 6px 8px;
    }
  }
</style>
