'use strict';

import config from './config'
import CATEGORIES from './categories'
import { ValidationError } from './utils/errors'
import logger from './utils/logger'

const log = logger.child({module: 'deliverer_factory'})

export default (opts) => {
  const options = Object.assign({}, opts)
  const { impl } = options

  return {
    queue: async (category, data) => {
      // TBD apply data points sanitization

      // only the category of system can be broadcast
      if ((!data.app_id || !data.device_id) && category !== CATEGORIES.SYSTEM)
        throw new ValidationError('missing app_id/device_id or both')

      if (!data.message || typeof data.message !== 'string')
        throw new ValidationError('missing message')

      const app_id = data.app_id
      const device_id = data.device_id
      const msg = {
        message: data.message,
        // blob represents application specific data
        blob: data.blob
        // TBD set message expiry? default?
      }

      const err_msg = 'error delivering message'
      if(app_id && device_id) {
        log.debug({ message: msg }, `sending ${category} message to ${app_id}-${device_id}`)
        try {
          const target = { app_id: app_id, device_id: device_id }
          return await impl.send(target, category, msg)
        } catch (err) {
          log.error(err, 'error sending message')
          throw new Error(err_msg)
        }
      }
      else {
        log.debug({ message: msg }, `broadcasting ${category} message`)
        try {
          return await impl.broadcast(category, msg)
        } catch (err) {
          log.error(err, 'error broadcasting message')
          throw new Error(err_msg)
        }
      }
    }
  }
}
