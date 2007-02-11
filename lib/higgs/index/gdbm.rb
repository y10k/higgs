# $Id$

require 'gdbm'
require 'higgs/index'

module Higgs
  module Index
    # for ident(1)
    GDBM_CVS_ID = '$Id$'

    GDBM_F_MODE = 0660
    GDBM_R_FLAGS = GDBM::READER | GDBM::NOLOCK
    GDBM_W_FLAGS = GDBM::WRCREAT

    GDBM_READ_OPEN = proc{|name| GDBM.open(name, MODE, R_FLAGS) }
    GDBM_WRITE_OPEN = proc{|name| GDBM.open(name, MODE, W_FLAGS) }
  end
end
