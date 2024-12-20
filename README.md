# nyc-transit

The app presents interactive data on a new measure of **public transit accessibility to public services** calculated using a route optimization engine. The specific use case presented here is for public transit accessibility to higher education institutions from Brooklyn, using Google's [Directions API](https://developers.google.com/maps/documentation/directions/overview). 

The methods utilized are easily transferable to other use cases (e.g. hospitals, aged-care, and community service centres) and may be used with alternative route optimizers (e.g. OpenStreetMaps Routing Machine or Radar). 

The data here are generated by requesting route information on approximately 500,000 individual trips between block group centroids and higher education institutions.

#### The purpose of this app is threefold:

1) To showcase a new approach to measuring transit accessibility in the context of higher education institutions;
2) To provide a toolkit for basic interactive exploratory work; and
3) To construct a platform to share meaningful data so that policymakers and researchers interested in the public transit-higher education nexus may conduct further work

![image-20221231071845169](https://github.com/ltk2118/nyc-transit/blob/main/app-screen.png)

#### Please note, for cost reasons, the app is no longer hosted on shinyapps.io. It may be replicated locally using the code base provided in this repo.
