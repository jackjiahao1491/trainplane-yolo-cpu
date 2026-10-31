<script setup lang="ts">
import { computed } from 'vue'
import { useRoute } from 'vue-router'
import AppIcon from './components/AppIcon.vue'
const route = useRoute()
const title = computed(() => route.meta.title || '训练平台')
const links = [
  { to: '/overview', name: '集群总览', icon: 'grid', number: '01' },
  { to: '/jobs', name: '任务管理', icon: 'jobs', number: '02' },
  { to: '/submit', name: '提交任务', icon: 'plus', number: '03' },
]
</script>

<template>
  <div class="app-shell">
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
        <div class="topbar-right"><span class="environment-badge"><span class="status-dot" />CPU ONLY</span><span class="avatar" aria-label="本地用户">L</span></div>
      </header>
      <main class="main-content"><RouterView /></main>
      <footer class="page-footer"><span>TrainPlane / 轻量化训练平台</span><span>本地数据 · CPU 训练 · 实时观察</span></footer>
    </div>
  </div>
</template>
