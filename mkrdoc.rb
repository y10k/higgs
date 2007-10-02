#!/usr/local/bin/ruby

# for ident(1)
CVS_ID = '$Id$'

require 'fileutils'
require 'yaml'

rdoc_opts = YAML.load(IO.read('rdoc.yml'))
FileUtils.rm_rf(rdoc_opts['CommandLineOptions'].assoc('-o')[1], :verbose => true)
system('rdoc', *(rdoc_opts['CommonOptions'] + rdoc_opts['CommandLineOptions']).flatten)
