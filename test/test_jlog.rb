#!/usr/local/bin/ruby

require 'fileutils'
require 'higgs/block'
require 'higgs/jlog'
require 'test/unit'

module Higgs::Test
  class JournalLoggerTest < Test::Unit::TestCase
    include Higgs

    # for ident(1)
    CVS_ID = '$Id$'

    def setup
      @path = 't.jlog'
    end

    def teardown
      FileUtils.rm_f(@path)
    end

    def test_jlog
      log = JournalLogger.open(@path)
      assert_equal(false, log.sync?)
      log.close

      JournalLogger.each_log(@path) do |log|
        assert_fail('not to reach')
      end

      log = JournalLogger.open(@path, false)
      assert_equal(false, log.sync?)
      log.write('foo')
      log.close

      count = 0
      expected_values = [ 'foo' ]
      JournalLogger.each_log(@path) do |log|
        nth = "loop: #{count}"
        assert(! expected_values.empty?, nth)
        assert_equal(expected_values.shift, log, nth)
        count += 1
      end
      assert(expected_values.empty?)

      log = JournalLogger.open(@path, true)
      assert_equal(true, log.sync?)
      log.write(:bar)
      log.close

      count = 0
      expected_values = [ 'foo', :bar ]
      JournalLogger.each_log(@path) do |log|
        nth = "loop: #{count}"
        assert(! expected_values.empty?, nth)
        assert_equal(expected_values.shift, log, nth)
        count += 1
      end
      assert(expected_values.empty?)

      log = JournalLogger.open(@path, false)
      assert_equal(false, log.sync?)
      log.write(777)
      log.close

      count = 0
      expected_values = [ 'foo', :bar, 777 ]
      JournalLogger.each_log(@path) do |log|
        nth = "loop: #{count}"
        assert(! expected_values.empty?, nth)
        assert_equal(expected_values.shift, log, nth)
        count += 1
      end
      assert(expected_values.empty?)

      log = JournalLogger.open(@path, true)
      assert_equal(true, log.sync?)
      log.write('X' * 512)
      log.write('Y' * 513)
      log.close

      count = 0
      expected_values = [ 'foo', :bar, 777, 'X' * 512, 'Y' * 513  ]
      JournalLogger.each_log(@path) do |log|
        nth = "loop: #{count}"
        assert(! expected_values.empty?, nth)
        assert_equal(expected_values.shift, log, nth)
        count += 1
      end
      assert(expected_values.empty?)
    end

    def test_eof_mark
      assert_equal(true, (JournalLogger.has_eof_mark? @path))

      FileUtils.touch(@path)
      assert_equal(false, (JournalLogger.has_eof_mark? @path))

      File.open(@path, 'w') {|w|
        w.binmode
        w << 'x' * 512
        JournalLogger.eof_mark(w)
      }
      assert_equal(true, (JournalLogger.has_eof_mark? @path))

      File.truncate(@path, File.stat(@path).size - 1)
      assert_equal(false, (JournalLogger.has_eof_mark? @path))
    end

    def test_open_error
      log = JournalLogger.open(@path)
      log.write(:foo)
      log.close(false)

      assert_raise(RuntimeError) {
        JournalLogger.open(@path)
      }
    end

    def test_scan_log_error
      log = JournalLogger.open(@path)
      log.write(:foo)
      log.write(:bar)
      log.write(:baz)
      log.close(false)
      File.truncate(@path, File.stat(@path).size - 1)

      File.open(@path) {|r|
        r.binmode
        count = 0
        expected_values = [ :foo, :bar ]
        assert_raise(Block::BrokenError) {
          JournalLogger.scan_log(r) do |log|
            nth = "loop: #{count}"
            assert_equal(expected_values.shift, log, nth)
            count += 1
          end
        }
        assert(expected_values.empty?)
        assert_equal(2048, r.tell)
      }
    end
  end
end

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
