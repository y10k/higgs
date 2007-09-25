# like cgi/session/pstore.rb
#
# Author:: $Author$
# Date:: $Date$
# Revision:: $Revision$
#

require 'cgi/session'
require 'digest/md5'
require 'fileutils'
require 'higgs/store'

class CGI
  class Session
    # like cgi/session/pstore.rb
    class HiggsStore
      # for ident(1)
      CVS_ID = '$Id$'

      def initialize(session, options={})
        dir = options['tmpdir'] || Dir.tmpdir
        prefix = options['prefix'] || ''
        id = session.session_id
        name = options['name'] || 'session'
        md5 = Digest::MD5.hexdigest(id)
        @store_dir = File.join(dir, prefix + md5)
        FileUtils.mkdir_p(@store_dir)
        @store_path = File.join(@store_dir, name)
        @store_path.untaint
        if (File.exist? "#{@store_path}.lock") then
          @hash = nil
        else
          unless (session.new_session) then
            raise CGI::Session::NoSession, 'uninitialized session'
          end
          @hash = {}
        end
        @store = Higgs::Store.new(@store_path, options)
      end

      def restore
        unless (@hash) then
          @store.transaction{|tx|
            @hash = tx[:hash] || {}
          }
        end
        @hash
      end

      def update
        @store.transaction{|tx|
          tx[:hash] = @hash
        }
      end

      def close
        r = nil
        unless (@store.shutdown?) then
          r = update
          @store.shutdown
        end
        r
      end

      def delete
        @store.shutdown unless @store.shutdown?
        FileUtils.rm_rf(@store_dir)
        nil
      end
    end
  end
end
