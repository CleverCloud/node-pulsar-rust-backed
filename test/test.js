const pulsarnative = require('../index.js');
const { GenericContainer, Wait } = require("testcontainers");

describe("Pulsar test container", () => {
  let container;
  let pulsar;
  let producer;

  beforeAll(async () => {
    jest.setTimeout(60000);
    container = await new GenericContainer("apachepulsar/pulsar:2.9.1")
      .withExposedPorts(6650)
      .withCmd(["/pulsar/bin/pulsar", "standalone"])
      .withHealthCheck({
        test: "curl -f http://localhost:8080/admin/v2/persistent/public/default/ || exit 1",
        interval: 5000,
        timeout: 1000,
        retries: 5,
        startPeriod: 5000
      })
      .withWaitStrategy(Wait.forHealthCheck())
      .start()

    pulsar = pulsarnative.createPulsar({
      url: `pulsar://127.0.0.1:${container.getMappedPort(6650)}`
    });
    producer = pulsarnative.createPulsarProducer(pulsar);
  })

  afterAll(async () => {
    console.log("Killing container")
    await container.stop();
  })

  it("should be able to produce and consume to and from default non persistent topic", async () => {

    let results = [];
    let messages = [
      "message 1",
      "message 2",
      "message 3",
      "message 4",
      "end"
    ]

    await new Promise(resolve => {

      pulsarnative.startPulsarConsumer(pulsar, function () {

        let message = arguments["1"];
        results.push(message);

        if (message === "end") {
          resolve()
        }
      }, {});
      
      for (let message of messages) {
        pulsarnative.sendPulsarMessage(producer, { message});
      }

    
    })
    expect(results).toEqual(expect.arrayContaining(messages))

  })
})