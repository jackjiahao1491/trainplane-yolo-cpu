<script setup lang="ts">
import { statusLabels, type Status } from '../api'
defineProps<{ status: Status; cancelling?: boolean }>()
</script>

<template>
  <span :class="['status-badge', status]"><span class="status-dot" />{{ cancelling ? '正在停止' : statusLabels[status] || status }}</span>
</template>
