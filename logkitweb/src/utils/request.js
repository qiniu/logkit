import 'isomorphic-fetch';
import {notification,} from 'antd';

function parseJSON(response) {
  return response.json();
}

function checkStatus(response) {
  if (response.status >= 200 && response.status < 300) {
    return response;
  } else {
    return response;
  }
}

/**
 * Requests a URL, returning a promise.
 *
 * @param  {string} url       The URL we want to request
 * @param  {object} [options] The options we want to pass to "fetch"
 * @return {object}           An object containing either "data" or "err"
 */
export default function request(url, options) {
  return fetch(url, options)
      .then(checkStatus)
      .then(parseJSON)
      .then((data) => {
        if (data.code === 'L200') {
          return data;
        } else {
          notification.error({message: "失败", description: data.message, duration: 20})
          return data
        }
      })
      .catch(err => {
        return err
      });
}
