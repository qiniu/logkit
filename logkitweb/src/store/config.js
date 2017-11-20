const config = {};

window.nodes = {}

config.get = (type) => {
  return window.nodes[type]
};

config.set = (type, data) => {
  window.nodes[type] = data
};

config.delete = (type) => {
  delete window.nodes[type]
};

config.getNodeData = () => {
  return window.nodes
};

config.setNodeData = (data) => {
  return window.nodes = data
};

export default config