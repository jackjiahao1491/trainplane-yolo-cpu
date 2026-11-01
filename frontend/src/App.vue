<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { useRoute } from 'vue-router'
import AppIcon from './components/AppIcon.vue'
import { restoreSession, session, signIn, signOut } from './api'
const route = useRoute()
const title = computed(() => route.meta.title || '训练平台')
const links = [
  { to: '/overview', name: '集群总览', icon: 'grid', number: '01' },
  { to: '/jobs', name: '任务管理', icon: 'jobs', number: '02' },
  { to: '/submit', name: '提交任务', icon: 'plus', number: '03' },
]
const token = ref('')
const loginError = ref('')
const signingIn = ref(false)
async function login() {
  if (signingIn.value) return
  signingIn.value = true
  loginError.value = ''
  try {
    await signIn(token.value)
    token.value = ''
  } catch (error) {
    token.value = ''
    loginError.value = error instanceof Error ? error.message : '登录失败，请重试'
  } finally { signingIn.value = false }
}
onMounted(async () => {
  try { await restoreSession() }
  catch (error) { loginError.value = error instanceof Error ? error.message : '会话验证失败，请重试' }
})
</script>

<template>
  <div v-if="!session.authenticated" class="auth-shell">
    <section class="auth-card" aria-labelledby="login-title">
      <div class="auth-brand"><span class="brand-symbol"><AppIcon name="cpu" :size="25" /></span><span>TrainPlane<small>轻量化训练平台</small></span></div>
      <div class="eyebrow">LOCAL WORKSPACE</div>
      <h1 id="login-title">进入训练工作空间</h1>
      <p>使用访问令牌登录，管理任务、资源与训练日志。</p>
      <div v-if="session.checking" class="auth-checking" role="status">正在验证会话…</div>
      <el-form v-else class="auth-form" label-position="top" novalidate @submit.prevent="login">
        <el-form-item label="访问令牌"><el-input v-model="token" type="password" show-password autocomplete="off" placeholder="输入管理员提供的访问令牌" aria-label="访问令牌" :disabled="signingIn" /></el-form-item>
        <el-alert v-if="loginError" :title="loginError" type="error" :closable="false" show-icon />
        <el-button type="primary" size="large" native-type="submit" :loading="signingIn" :disabled="!token.trim()">进入工作空间<AppIcon name="arrow" :size="16" /></el-button>
      </el-form>
      <div class="auth-footnote">令牌仅保存在当前标签页会话中，可通过退出登录清除。</div>
    </section>
    <footer class="auth-footer">TrainPlane · CPU TRAINING</footer>
  </div>
  <div v-else class="app-shell">
    <aside class="sidebar">
      <RouterLink to="/overview" class="brand" aria-label="TrainPlane 首页">
        <span class="brand-symbol"><AppIcon name="cpu" :size="25" /></span>
        <span>TrainPlane<small>轻量化训练平台</small></span>
      </RouterLink>
      <div class="workspace-label">WORKSPACE <span>本地工作空间</span></div>
      <nav aria-label="主导航">
        <RouterLink v-for="link in links" :key="link.to" :to="link.to" :aria-label="link.name" :class="['nav-link', { selected: route.path === link.to || (link.to === '/jobs' && route.path.startsWith('/jobs/')) }]">
          <AppIcon :name="link.icon" /><span>{{ link.name }}</span><small>{{ link.number }}</small>
        </RouterLink>
      </nav>
      <div class="sidebar-note"><div class="small-cpu"><AppIcon name="cpu" :size="19" />CPU TRAINING</div><p>让每一次训练<br />都有迹可循。</p><span>YOLO · Ray · 本地执行</span></div>
      <div class="sidebar-footer"><span class="tiny-light" /> 单节点工作空间 <span class="version">v1.0</span></div>
    </aside>
    <div class="main-shell">
      <header class="topbar">
        <div class="breadcrumbs">工作空间 <span>/</span> <strong>{{ title }}</strong></div>
        <div class="topbar-right"><span class="environment-badge"><span class="status-dot" />CPU ONLY</span><span class="session-user">{{ session.username }}</span><el-button text size="small" class="logout-button" @click="signOut">退出登录</el-button></div>
      </header>
      <main class="main-content"><RouterView /></main>
      <footer class="page-footer"><span>TrainPlane / 轻量化训练平台</span><span>本地数据 · CPU 训练 · 实时观察</span></footer>
    </div>
  </div>
</template>
