#!/usr/local/bin/ruby

require 'higgs/index/gdbm'
require 'rubyunit'
require 'utils_storage'

module Higgs::StorageTest
  class StorageGDBMTest < RUNIT::TestCase
    # for ident(1)
    CVS_ID = '$Id$'

    include StorageTest

    def dbm_open
      Higgs::Index::GDBM_OPEN
    end
  end
end
