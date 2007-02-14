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

    GDBM_OPEN = {
      :read => proc{|name| GDBM.open(name, GDBM_F_MODE, GDBM_R_FLAGS) },
      :write => proc{|name| GDBM.open(name, GDBM_F_MODE, GDBM_W_FLAGS) }
    }
  end
end
