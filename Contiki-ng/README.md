# Evaluation lab - Contiki-NG

## A Simple Sensing Application

You are to implement a simple sensing application
– A UDP client sends (fake) temperature readings to a
server every ~1 minute
– The UDP server collects the last MAX_READINGS
temperature readings coming from clients
– Every time a new reading is received, the server
• Computes the average of the received temperature readings
• If the average is above ALERT_THRESHOLD, it
immediately notifies back-to-back all existing clients of a
“high temperature alert”
– Nothing is sent back otherwise

### Considerations

Clients may appear at any time, but they never
disappear
• Your solution must be able to handle up to
MAX_RECEIVERS clients, no more
• You can use COOJA motes
• Multiple calls to simple_udp_sendto in
sequence are unlikely to succeed
– The outgoing queue has size one

