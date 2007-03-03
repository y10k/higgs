#!/usr/local/bin/ruby

require 'higgs/index/gdbm'
require 'rubyunit'
require 'utils_dbm'

module Higgs::DBMTest
  class DBMTest_GDBM < RUNIT::TestCase
    # for ident(1)
    CVS_ID = '$Id$'

    include DBMTest

    def dbm_open
      Higgs::Index::GDBM_OPEN
    end
  end
end
