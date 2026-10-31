<script setup lang="ts">
withDefaults(defineProps<{ name: string; size?: number }>(), { size: 20 })
const paths: Record<string, string> = {
  grid: 'M3 3h7v7H3z M14 3h7v7h-7z M3 14h7v7H3z M14 14h7v7h-7z',
  jobs: 'M9 5h11v15H4V5h3 M8 3h8v4H8z M8 11h8 M8 15h5',
  plus: 'M12 5v14 M5 12h14',
  cpu: 'M7 7h10v10H7z M10 10h4v4h-4z M8 3v4 M12 3v4 M16 3v4 M8 17v4 M12 17v4 M16 17v4 M3 8h4 M3 12h4 M3 16h4 M17 8h4 M17 12h4 M17 16h4',
  arrow: 'M5 12h14 M13 6l6 6-6 6',
  refresh: 'M20 7v5h-5 M4 17v-5h5 M5.5 8a7 7 0 0 1 12-3L20 8 M4 16l2.5 3a7 7 0 0 0 12-3',
  search: 'M10.5 17a6.5 6.5 0 1 0 0-13 6.5 6.5 0 0 0 0 13z M15.5 15.5 21 21',
  play: 'M8 4l12 8-12 8z',
  clock: 'M12 22a10 10 0 1 0 0-20 10 10 0 0 0 0 20z M12 6v6l4 2',
  terminal: 'M4 6l5 6-5 6 M12 18h8',
  back: 'M19 12H5 M11 6l-6 6 6 6',
  copy: 'M9 9h12v12H9z M15 9V3H3v12h6',
  pause: 'M8 5v14 M16 5v14',
  check: 'M5 12l4 4L19 6',
  book: 'M4 4h7a3 3 0 0 1 3 3v14a3 3 0 0 0-3-3H4z M14 7a3 3 0 0 1 3-3h4v14h-4a3 3 0 0 0-3 3',
}
</script>

<template>
  <svg :width="size" :height="size" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path :d="paths[name] || paths.grid" /></svg>
</template>
