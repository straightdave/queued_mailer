require 'redis'
require 'json'
require 'pony'
require 'mysql2'

trap("INT") {
  puts "=== wait for child process to exit ..."
  wait
  puts "=== XXX Mailer gracefully exit"
  exit
}

#===========
# config
#===========
module RedisConf
  Host = '127.0.0.1'
  Port = 6379
  DB   = 8
  List = "validation"
end

module PonyConf
  Charset = 'utf-8'
  SMTP_Options = {
    :address        => '######',
    :port           => '25',
    :user_name      => '######',
    :password       => '######',
    :authentication => :plain,
    :domain         => '######'
  }
end

module MySQLConf
  Host = 'localhost'
  User = '#######'
  Pass = '#######'
  DB   = 'xxx'
end

#==================
# main process
#==================
begin
  puts "[mailer] #{Time.now} : init Redis client ..."
  rc = Redis.new(
    :host => RedisConf::Host,
    :port => RedisConf::Port,
    :db   => RedisConf::DB
  )

  puts "[mailer] #{Time.now} : init Mysql2 client ... "
  mc = Mysql2::Client.new(
    :host     => MySQLConf::Host,
    :username => MySQLConf::User,
    :password => MySQLConf::Pass,
    :database => MySQLConf::DB
  )

  puts "[mailer] #{Time.now} : running child, see child's log ... "
rescue
  puts $!.message
  puts "[mailer] #{Time.now} : exit ..."
  exit
end

include Process

pid = fork do
  puts "[mailer child] #{Time.now} : init logging file ..."
  log_file = File.new(File.dirname(__FILE__) + "/log/mailer.log", 'a+')
  log_file.sync = true

  log_file.puts
  log_file.puts "=============================================="
  log_file.puts "[mailer child] #{Time.now} : init logging file ..."

  trap("INT") {
    log_file.puts "[mailer child] #{Time.now} : get ctrl-c, exit ..."
    log_file.close unless log_file.nil?
    exit
  }

  trap("TERM") {
    puts "[mailer child] #{Time.now} : get TERM signal, exit ..."
    log_file.close unless log_file.nil?
    exit
  }

  while true
    begin
      puts "[mailer child] #{Time.now} : reading redis queue for next job ..."
      log_file.puts "[mailer child] #{Time.now} : reading redis queue for next job ..."

      job = rc.brpop("validation")
      log_file.puts "[mailer child] #{Time.now} : job get =>"

      mail = JSON.parse(job[1])
      log_file.puts "  list name: #{job[0]}, mail: #{mail}"
      log_file.puts "  gonna send mail to #{mail["to"]} ..."

      Pony.mail(
        :to          => mail["to"],
        :from        => mail["from"],
        :subject     => mail["subject"],
        :html_body   => mail["body"],
        :charset     => PonyConf::Charset,
        :via         => :smtp,
        :via_options => PonyConf::SMTP_Options
      )
      log_file.puts "  mail sent successfully!"

      if mail["log_id"]
        log_file.puts "  gonna update status in mail_logs for id = #{ mail["log_id"] } ..."
        mc.query("UPDATE mail_logs SET status = 2, updated_at = now() WHERE id = #{ mail["log_id"] }")
      end
    rescue Redis::CannotConnectError => e
      log_file.puts $!.message
      log_file.puts "[mailer child] #{Time.now} : exit ..."
      puts "[mailer child] #{Time.now} : exit since fatal redis issue ..."
      exit
    rescue
      log_file.puts "[mailer child] #{Time.now} : Exception in waiting loop:"
      log_file.puts $!.message
    end
  end
end

wait
