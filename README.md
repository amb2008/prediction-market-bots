# Prediction Market Bots

Source code and historical versions of prediction-market arbitrage scripts.

Aldrovanda 2.41 was most successful, achieving profits of $0.70/day on $10 of capital. It uses a hybrid maker-taker arbitrage strategy, setting rapidly updating maker orders, such that if one gets filled, a taker hedge can be placed on the opposite side of the market at an x-cent spread (3 cent spread was best in testing). I named it "Aldrovanda" after the Aldrovanda Vesiculosa plant, which is an aquatic carnivorous plant that has snapping leaves. 

Aldrovanda 2.50 was a less successful iteration after 2.41, which attempted to increase profit by setting layers of maker orders simultaneously, however the increased complexity led to larger latency, offsetting theoretical profit gains.

The implemented methodology is all function and follows the theory that I developed, however bottlenecks in efficiency pose challenges to scale. If the market shifts between the time that the maker is filled and the taker is placed, the arbitrage may be gone, or you may end up paying more than the guaranteed return. This problem is largely avoidable at low volumes, but it is inevitable with any meaningful trading size.

Other scripts in this repository include experiments with similar methodology applied to polymarket, along with a polymarket-kalshi cross-market arbitrage. Polymarket's fee structure rendered most of these attempts unsuccesful.

Credential values in the scripts have been replaced with dummy placeholders.
Provide credentials securely outside the source before running any script.

This repository intentionally excludes local environments, trading data, logs,
archives, notebooks, and the separately versioned `flashscore-scraper` project.
