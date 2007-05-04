#!/usr/local/bin/ruby

require 'fileutils'
require 'higgs/jlog'
require 'test/unit'

module Higgs::JournalTest
  # for ident(1)
  CVS_ID = '$Id$'

  class JournalLoggerTest < Test::Unit::TestCase
    include Higgs

    def setup
      @path = 't.jlog'
    end

    def teardown
      FileUtils.rm_f(@path)
    end

    def test_jlog
      log = JournalLogger.open(@path)
      log.close

      JournalLogger.each_log(@path) do |log|
	assert_fail('not to reach')
      end

      log = JournalLogger.open(@path)
      log.write('foo')
      log.close

      count = 0
      expected_values = [ 'foo' ]
      JournalLogger.each_log(@path) do |log|
	nth = "loop: #{count}"
	assert(! expected_values.empty?, nth)
	assert_equal(expected_values.shift, log, nth)
      end
      assert(expected_values.empty?)

      log = JournalLogger.open(@path)
      log.write(:bar)
      log.close

      count = 0
      expected_values = [ 'foo', :bar ]
      JournalLogger.each_log(@path) do |log|
	nth = "loop: #{count}"
	assert(! expected_values.empty?, nth)
	assert_equal(expected_values.shift, log, nth)
      end
      assert(expected_values.empty?)

      log = JournalLogger.open(@path)
      log.write(777)
      log.close

      count = 0
      expected_values = [ 'foo', :bar, 777 ]
      JournalLogger.each_log(@path) do |log|
	nth = "loop: #{count}"
	assert(! expected_values.empty?, nth)
	assert_equal(expected_values.shift, log, nth)
      end
      assert(expected_values.empty?)

      log = JournalLogger.open(@path)
      log.write('X' * 512)
      log.write('Y' * 513)
      log.close

      count = 0
      expected_values = [ 'foo', :bar, 777, 'X' * 512, 'Y' * 513  ]
      JournalLogger.each_log(@path) do |log|
	nth = "loop: #{count}"
	assert(! expected_values.empty?, nth)
	assert_equal(expected_values.shift, log, nth)
      end
      assert(expected_values.empty?)
    end
  end
end
