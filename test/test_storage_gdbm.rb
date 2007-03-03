#!/usr/local/bin/ruby

require 'case_storage'
require 'higgs/index/gdbm'
require 'rubyunit'

module Higgs::StorageTest
  class StorageTest_GDBM < RUNIT::TestCase
    # for ident(1)
    CVS_ID = '$Id$'

    include StorageTestCase

    def dbm_open
      Higgs::Index::GDBM_OPEN
    end
  end
end
