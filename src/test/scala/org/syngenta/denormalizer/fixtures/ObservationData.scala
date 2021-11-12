package org.syngenta.denormalizer.fixtures

object ObservationData {

  val OBS_VALID_EVENT: String =
    """ {"obsCode":"E_WIND_SPEED","codeComponents":[{"componentCode":"CC_AGG_TIME_HOUR","componentType":"AGG_TIME_WINDOW","selector":"DURATION","value":"3",
      | "valueUoM":"hr"},{"componentCode":"CC_AGG_METHOD_MEAN","componentType":"AGG_METHOD","selector":"MEAN"}],
      | "valueUoM":"km1hr-1","value":"5.04","id":"dcc28e61-7a93-4aec-ab9d-116ac1f58e40","parentCollectionRef":"f053617f-0072-11ec-b0d5-5d80d3cc2890",
      | "integrationAccountRef":"17af3a6a-440c-46e8-bd20-c16d69226a9b_f61bb604-cd1e-4f70-94ab-6ad8ae97a488","assetRef":"00002DE10","xMin":-48.171895,"xMax":-48.171895,"yMin":-18.936781,"yMax":-18.936781,
      |"phenTime":"2020-08-01T00:00:00Z","spatialExtent":"{\"type\": \"Point\", \"coordinates\":  [-30.171895, -20.936781]}"}""".stripMargin

  val OBS_VALID_EVENT_2: String =
    """ {"obsCode":"E_WIND_SPEED","codeComponents":[{"componentCode":"CC_AGG_TIME_HOUR","componentType":"AGG_TIME_WINDOW","selector":"DURATION","value":"3",
      | "valueUoM":"hr"},{"componentCode":"CC_AGG_METHOD_MEAN","componentType":"AGG_METHOD","selector":"MEAN"}],
      | "valueUoM":"km1hr-1","value":"5.04","id":"dcc28e61-7a93-4aec-ab9d-116ac1f58e40","parentCollectionRef":"f053617f-0072-11ec-b0d5-5d80d3cc2890",
      | "integrationAccountRef":"17af3a6a-440c-46e8-bd20-c16d69226a9b_f61bb604-cd1e-4f70-94ab-6ad8ae97a488","assetRef":"00002DE11","xMin":-48.171895,"xMax":-48.171895,"yMin":-18.936781,"yMax":-18.936781,
      |"phenTime":"2020-08-01T00:00:00Z","spatialExtent":"{\"type\": \"Point\", \"coordinates\":  [-30.171895, -20.936781]}"}""".stripMargin

}
