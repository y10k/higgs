# = storage like dbm
#
# Author:: $Author$
# Date:: $Date$
# Revision:: $Revision$
#
# == license
#   :include:LICENSE
#

require 'higgs/storage'
require 'higgs/tman'

module Higgs
  # = storage like dbm
  # == sample script
  #
  # sample/dbmtest.rb
  #   :include: sample/dbmtest.rb
  #
  # result of sample script.
  #   % ruby dbmtest.rb
  #   % ruby dbmtest.rb
  #   -
  #   key: quux
  #   value: QX
  #   system_property[hash_type]: MD5
  #   system_property[modified_time]: Tue Oct 02 00:52:58 +0900 2007
  #   system_property[created_time]: Tue Oct 02 00:52:58 +0900 2007
  #   system_property[hash_value]: 2e8e88f56e5d52ce42f59592efbc2831
  #   system_property[changed_time]: Tue Oct 02 00:52:58 +0900 2007
  #   system_property[string_only]: false
  #   custom_property[number]: 2
  #   -
  #   key: baz
  #   value: BZ
  #   system_property[hash_type]: MD5
  #   system_property[modified_time]: Tue Oct 02 00:52:58 +0900 2007
  #   system_property[created_time]: Tue Oct 02 00:52:58 +0900 2007
  #   system_property[hash_value]: e45fbcf6ca3b21f17c5f355728a2fbec
  #   system_property[changed_time]: Tue Oct 02 00:52:58 +0900 2007
  #   system_property[string_only]: false
  #   custom_property[number]: 1
  #   -
  #   key: foobar
  #   value: FB
  #   system_property[hash_type]: MD5
  #   system_property[modified_time]: Tue Oct 02 00:52:58 +0900 2007
  #   system_property[created_time]: Tue Oct 02 00:52:58 +0900 2007
  #   system_property[hash_value]: 30781f1fc2f9342ceb1ad2f6f35a51db
  #   system_property[changed_time]: Tue Oct 02 00:52:58 +0900 2007
  #   system_property[string_only]: false
  #   custom_property[number]: 0
  #   %
  #
  class DBM
    # for ident(1)
    CVS_ID = '$Id$'

    include Storage::Export
    include TransactionManager::Export

    DECODE = proc{|r| r }
    ENCODE = proc{|w| w }

    # <tt>name</tt> is a storage name and see Higgs::Storage.new for
    # detail. see Higgs::Storage::InitOptions and
    # Higgs::TransactionManager::InitOptions for <tt>options</tt>.
    def initialize(name, options={})
      @storage = Storage.new(name, options)
      options[:decode] = DECODE
      options[:encode] = ENCODE
      @tman = TransactionManager.new(@storage, options)
    end

    def self.open(*args)
      dbm = new(*args)
      begin
        r = yield(dbm)
      ensure
        dbm.shutdown
      end
      r
    end
  end
end

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
