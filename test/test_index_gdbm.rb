#!/usr/local/bin/ruby

require 'higgs/index/gdbm'
require 'rubyunit'
require 'utils_index'

module Higgs::IndexTest
  # for ident(1)
  GDBM_CVS_ID = '$Id$'

  class GDBMTest < RUNIT::TestCase
    include Higgs::Index
    include IndexTest

    def db_read_open
      db = GDBM_READ_OPEN.call(@name)
      begin
        r = yield(db)
      ensure
        db.close
      end
      r
    end

    def db_write_open
      db = GDBM_WRITE_OPEN.call(@name)
      begin
        r = yield(db)
      ensure
        db.close
      end
      r
    end
  end
end
