#!/usr/local/bin/ruby

require 'case_dbm'
require 'higgs/index/qdbm'
require 'rubyunit'

module Higgs::DBMTest
  class DBMTest_QDBM < RUNIT::TestCase
    # for ident(1)
    CVS_ID = '$Id$'

    include DBMTestCase

    def dbm_open
      Higgs::Index::QDBM_OPEN
    end
  end
end
