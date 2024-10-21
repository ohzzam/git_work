import Vue from 'vue'
import Router from 'vue-router'
import JobManager from '@/components/JobManager'

Vue.use(Router)

export default new Router({
  routes: [
    {
      path: '/',
      name: 'JobManager',
      component: JobManager
    }
  ]
})
