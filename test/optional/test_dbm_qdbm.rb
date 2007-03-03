#!/usr/local/bin/ruby

require 'higgs/index/qdbm'
require 'rubyunit'
require 'utils_dbm'

module Higgs::DBMTest
  class DBMTest_QDBM < RUNIT::TestCase
    # for ident(1)
    CVS_ID = '$Id$'

    include DBMTest

    def dbm_open
      Higgs::Index::QDBM_OPEN
    end
  end
end
