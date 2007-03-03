#!/usr/local/bin/ruby

require 'case_dbm'
require 'higgs/index/gdbm'
require 'rubyunit'

module Higgs::DBMTest
  class DBMTest_GDBM < RUNIT::TestCase
    # for ident(1)
    CVS_ID = '$Id$'

    include DBMTestCase

    def dbm_open
      Higgs::Index::GDBM_OPEN
    end
  end
end
