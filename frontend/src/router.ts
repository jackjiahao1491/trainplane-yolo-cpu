import { createRouter, createWebHistory } from 'vue-router'
import Overview from './views/Overview.vue'
import Jobs from './views/Jobs.vue'
import Submit from './views/Submit.vue'
import JobDetail from './views/JobDetail.vue'

export default createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', redirect: '/overview' },
    { path: '/overview', component: Overview, meta: { title: '集群总览' } },
    { path: '/jobs', component: Jobs, meta: { title: '任务管理' } },
    { path: '/submit', component: Submit, meta: { title: '提交训练任务' } },
    { path: '/jobs/:id', component: JobDetail, meta: { title: '任务详情' } },
    { path: '/:pathMatch(.*)*', redirect: '/overview' },
  ],
})
