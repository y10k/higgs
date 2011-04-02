#!/usr/local/bin/ruby
# -*- coding: utf-8 -*-

require 'higgs/tman'
require 'test/unit'
require 'yaml'

module Higgs::Test
  class TransactionManagerInitOptionsTest < Test::Unit::TestCase
    include Higgs::TransactionManager::InitOptions

    def test_default
      init_options({})
      assert_equal(false, @read_only)
      assert_equal(false, self.read_only)
      assert_equal(:foo, @decode.call(:foo))
      assert_equal(:bar, @encode.call(:bar))
      assert_instance_of(Higgs::GiantLockManager, @lock_manager) # auto: require 'higgs/lock'
    end

    def test_read_only_true
      init_options(:read_only => true)
      assert_equal(true, @read_only)
      assert_equal(true, self.read_only)
    end

    def test_read_only_false
      init_options(:read_only => false)
      assert_equal(false, @read_only)
      assert_equal(false, self.read_only)
    end

    def test_decode
      init_options(:decode => proc{|r| YAML.load(r) })
      assert_equal([ 1, 2, 3], @decode.call([ 1, 2, 3 ].to_yaml))
    end

    def test_encode
      init_options(:encode => proc{|w| w.to_yaml })
      assert_equal([ 1, 2, 3 ].to_yaml, @encode.call([ 1, 2, 3 ]))
    end

    def test_lock_manager
      init_options(:lock_manager => :dummy_lock_manager)
      assert_equal(:dummy_lock_manager, @lock_manager)
    end
  end
end
