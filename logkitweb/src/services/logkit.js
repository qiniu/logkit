/**
 * Created by zhonghuiping on 2017/8/25.
 */
import request  from '../utils/request';

window.logkitUrl = ''

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
  return request(window.logkitUrl + '/logkit/configs', {
    method: 'get',
  });
}

export async function getTransformOptions(params) {
  return request(window.logkitUrl + '/logkit/transformer/options', {
    method: 'get',
  });
}

export async function getTransformConfigs(params) {
  return request(window.logkitUrl + '/logkit/transformer/sampleconfigs', {
    method: 'get',
  });
}

export async function getTransformUsages(params) {
  return request(window.logkitUrl + '/logkit/transformer/usages', {
    method: 'get',
  });
}


export async function getRunnerStatus(params) {
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

export async function putConfigData(params) {
  return request(window.logkitUrl + '/logkit/configs/' + params.name, {
    method: 'put',
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

export async function getRunnerVersion(params) {
  return request(window.logkitUrl + '/logkit/version', {
    method: 'get',
  });
}

export async function startRunner(params) {
    return request(window.logkitUrl + '/logkit/configs/' + params.name + '/start', {
        method: 'post',
        headers: {
            'Content-Type': 'application/json'
        }
    });
}

export async function stopRunner(params) {
    return request(window.logkitUrl + '/logkit/configs/' + params.name + '/stop', {
        method: 'post',
        headers: {
            'Content-Type': 'application/json'
        }
    });
}

export async function resetConfigData(params) {
  return request(window.logkitUrl + '/logkit/configs/' + params.name + '/reset', {
    method: 'post',
    headers: {
      'Content-Type': 'application/json'
    }
  });
}

