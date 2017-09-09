require 'dnssd'
require 'celluloid/current'
require 'celluloid/supervision'
require 'influxdb'

class Device
  include Celluloid

  def initialize(name)
    puts "Registered #{name}"
    @name = name
  end

  def run(influx_actor)
    # data looks like {"error"=>false, "temp"=>21.5, "humidity"=>60.5}
    data = JSON.parse(Net::HTTP.get("#{@name}.local", '/dht'))
    # get the data
    unless data['error']
      influx_actor.async.send(@name, data['temp'], data['humidity']);
    end
  end
end

class Store
  include Celluloid

  def initialize(table, db, config = {})
    @table = table
    @influx = InfluxDB::Client.new db, config
  end

  def send(name, temp, humidity)
    @influx.write_point(@table, {
      values: {
        humidity: humidity,
        temp: temp
      },
      tags: {
        sensor: name
      }
    })
  end
end

class Discovery
  MDNS_NAME = '_sensor._tcp.'.freeze
  SEARCH_FREQUENCY_SECONDS = 120

  include Celluloid
  attr_reader :discovered

  def initialize
    @discovered = []
    discover
    #@timer = every(SEARCH_FREQUENCY_SECONDS) { discover }
  end

  private

  #scan the network for sensors
  def discover
    puts "Looking for devices"
    current_devices = []
    DNSSD.browse MDNS_NAME do |reply|
      current_devices.push reply.name
    end
    @discovered = current_devices
  end
end

class Server

  LOG_FREQUENCY_SECONDS = 30
  include Celluloid

  def initialize
    puts "server started"
    Discovery.supervise as: :discovery
    Store.supervise as: :store, args: ["sensors", "phil"]
    @timer = every(LOG_FREQUENCY_SECONDS) { spawner }
  end

  def spawner
    Celluloid::Actor[:discovery].discovered.each do |device_name|
      Celluloid::Actor[device_name] ||= Device.new(device_name)
      Celluloid::Actor[device_name].async.run(Celluloid::Actor[:store])
    end
  end
end

Server.run
