/**
 * Created by zhonghuiping on 2017/7/7.
 */
import request  from '../utils/request';

/* window.logkitUrl = 'http://localhost:12581' */

export async function getSourceOptions(params) {
  return request(window.logkitUrl + '/logkit/reader/usages', {
    method: 'get',
  });
}

export async function getSourceOptionsFormData(params) {
  return request(window.logkitUrl + '/logkit/reader/options', {
    method: 'get',
  });
}

export async function getSourceParseOptionsFormData(params) {
  return request(window.logkitUrl + '/logkit/parser/options', {
    method: 'get',
  });
}

export async function getSourceParseOptions(params) {
  return request(window.logkitUrl + '/logkit/parser/usages', {
    method: 'get',
  });
}

export async function getSourceParsesamplelogs(params) {
  window.logkitUrl = 'http://localhost:12581'
  return request(window.logkitUrl + '/logkit/parser/samplelogs', {
    method: 'get',
  });
}

export async function getSenderOptionsFormData(params) {
  return request(window.logkitUrl + '/logkit/sender/options', {
    method: 'get',
  });
}

export async function getSenderOptions(params) {
  return request(window.logkitUrl + '/logkit/sender/usages', {
    method: 'get',
  });
}

export async function getRunnerConfigs(params) {
  return request(window.logkitUrl + '/logkit/status', {
    method: 'get',
  });
}

export async function postParseData(params) {
  return request(window.logkitUrl + '/logkit/parser/parse', {
    method: 'post',
    headers: {
      'Content-Type': 'application/json'
    },
    body: JSON.stringify(params.body),
  });
}

export async function postConfigData(params) {
  return request(window.logkitUrl + '/logkit/configs/' + params.name, {
    method: 'post',
    headers: {
      'Content-Type': 'application/json'
    },
    body: JSON.stringify(params.body),
  });
}

export async function deleteConfigData(params) {
  return request(window.logkitUrl + '/logkit/configs/' + params.name, {
    method: 'delete'
  });
}
