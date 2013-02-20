package = 'ludent'
version = 'scm-1'
source = {
  url = 'git://github.com/lipp/ludent.git',
}
description = {
  summary = 'A naive Lua indenter / formater / beautifier.',
  homepage = 'http://github.com/lipp/ludent',
  license = 'MIT/X11'
}
dependencies = {
  'lua >= 5.1',
}
build = {
  type = 'none',
  install = {
    bin = {
      'ludent',
    }
  }
}
