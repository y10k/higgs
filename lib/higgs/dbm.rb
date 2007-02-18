# $Id$

module Higgs
  class DBM
    # for ident(1)
    CVS_ID = '$Id$'

    module InitOptions
      def init_options(options)
        if (options.include? :read_only) then
          @read_only = options[:read_only]
        else
          @read_only = false
        end

        if (options.include? :storage_type) then
          @storage_type = options[:storage_type]
        else
          require 'higgs/storage'
          @storage_type = Higgs::Storage
        end

        if (options.include? :cache_type) then
          @cache_type = options[:cache_type]
        else
          require 'higgs/cache'
          @cache_type = Higgs::Cache::SharedWorkCache
        end

        if (options.include? :lock_manager) then
          @lock_manager = options[:lock_manager]
        else
          require 'higgs/lock'
          @lock_manager = Higgs::Lock::FineGrainLockManager.new
        end
      end
      private :init_options

      attr_reader :read_only
    end

    def initialize(name, options={})
      @name = name
      init_options(options)
      @storage = @storage_type.new(name, options)
    end
  end
end
