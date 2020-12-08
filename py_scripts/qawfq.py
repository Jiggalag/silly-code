import json

a = """[ {
         "value" : "64138.515625",
         "name" : "memorysize_mb",
         "certname" : "eu-smr-dev-01.inventale.com"
       }, {
         "value" : "0",
         "name" : "uptime_days",
         "certname" : "eu-smr-dev-01.inventale.com"
       }, {
         "value" : "61002.44140625",
         "name" : "memoryfree_mb",
         "certname" : "eu-smr-dev-01.inventale.com"
       }, {
         "value" : "10.101.101.150",
         "name" : "ipaddress",
         "certname" : "eu-smr-dev-01.inventale.com"
       }, {
         "value" : "12",
         "name" : "processorcount",
         "certname" : "eu-smr-dev-01.inventale.com"
       }, {
         "value" : "sync-18.20.1-v201808.1,sync-18.20.2,sync-18.20.2ds201810081422,sync-18.20.2ds201810091420,sync-18.20.2ds201810101421,sync-18.20.2-cpo28264.1,sync-18.20.2-dj-remove.2,sync-18.20.2-reportstest.3,sync-18.20.2-v201808.2,sync-18.20.2-v201808.3,sync-18.20.3,sync-18.20.4,sync-18.20.5,sync-18.21.0,sync-18.21.0ds201810111422,sync-18.21.0ds201810121422,sync-18.21.0ds201810131423,sync-18.21.0ds201810141422,sync-18.21.0-alzh-test.1,sync-18.21.0-spark-an.3,sync-18.21.0-timeout.1,sync-18.21.1,sync-18.21.1ds201810161421,sync-18.21.1ds201810171423,sync-18.21.1-cpo28044.3,sync-18.21.1-cpo28044.4,sync-18.21.1-cpo28044.5,sync-18.21.1-cpo28044.6,sync-18.21.1-cpo28225.1,sync-18.21.1-cpo-27410.1,sync-18.21.1-cpo-28251.1,sync-18.21.1-domain2.1,sync-18.21.2,sync-18.21.2ds201810181420,sync-18.21.2ds201810191421,sync-18.21.2ds201810201424,sync-18.21.2ds201810211422,sync-18.21.2-cpo28225.2,sync-18.21.2-cpo28225.3,sync-18.21.3,sync-18.21.3ds201810221420,sync-18.21.3ds201810231425,sync-18.21.3ds201810241423,sync-18.21.3-cpo28044.7,sync-18.21.3-cpo28331.1,sync-18.21.3-cpo-28325.1,sync-18.21.3-domain2.2,sync-18.21.3-utc.1,sync-18.22.0ds201810251424,sync-18.22.0-utc.2,sync-1.3.57-sim-to-csv.1,sync-1.3.57-sim-to-csv.2,sync-1.3.58-load-old-campaigns.1,sync-1.3.59-adfox-api.1,sync-1.3.59-adfox-api.2,sync-1.3.59-campaign-status.1,sync-1.3.59-detect-holes.1,sync-1.3.62,sync-1.3.63,sync-1.3.63-new-dfp-api.1,sync-1.3.63-sentry.1,sync-1.3.65-spike-pages.2,sync-1.3.67-fix-geo-npe.1,sync-1.3.70-dsp-jobs.1,sync-1.3.72-dsp-jobs.2,sync-1.3.72-dsp-jobs.3,sync-1.3.73-sample-sync-job.1,sync-1.3.73-sample-sync-job.2,sync-1.3.76-dsp-forecast.5,sync-1.3.78,sync-1.3.79-enriched-fix.1,sync-1.3.79-fix-zip.1,sync-1.3.80-testversion.4,sync-1.3.85-jdeb.2,sync-1.3.85-jdeb.5,sync-1.3.85-jdeb.6,sync-1.3.88,sync-1.3.89-expbackoff.1,sync-1.3.90,sync-1.3.92-report-checker.1,sync-1.3.92-report-checker.3,sync-1.3.92-report-checker.4,sync-1.3.93,sync-1.3.94,sync-1.3.94-invman.4,sync-1.3.94-invman.5,sync-1.3.94-invman.6,sync-1.3.94-invman.7,sync-1.3.94-invman.8,sync-1.3.94-invman.9,sync-1.3.94-invman.10,sync-1.3.95,sync-1.3.95-mark-deleted.1,sync-1.3.96,sync-1.3.96-spg-tag.1,sync-1.3.96-spg-tag.2,sync-1.3.96-spg-tag.3,sync-1.3.96-spg-tag.4,sync-1.3.96-spg-tag.5,sync-1.3.96-spg-tag.6",
         "name" : "sync_packages",
         "certname" : "eu-smr-dev-01.inventale.com"
       }, {
         "value" : "ifms-1.4.6-extrapolators.2,ifms-1.4.6-inv-manager.1,ifms-1.4.6-irving-caches.1,ifms-1.4.6-jdbc.1,ifms-1.4.6-loglevel-back.1,ifms-1.4.7,ifms-1.4.7-cpm-simulation.1,ifms-1.4.7-directory.1,ifms-1.4.7-inv-export.1,ifms-1.4.7-monthly-goal.1,ifms-1.4.7-non-comm.1,ifms-1.4.7-utf8props.1,ifms-1.4.7-utf8props.2,ifms-1.4.8,ifms-1.4.8-cpm-simulation.1,ifms-1.4.8-dsp-forecast.30,ifms-1.4.8-dsp-forecast.31,ifms-1.4.8-dsp-forecast.32,ifms-1.4.8-dsp-forecast.33,ifms-1.4.8-fscope.1,ifms-1.4.8-rick-api-metrics.1,ifms-1.4.8-salt.2,ifms-1.4.9,ifms-1.4.9-monitoring-columns.1,ifms-1.5.0,ifms-1.5.0-rick-monitoring.1,ifms-1.5.0-rick-monitoring.2,ifms-1.5.0-rick-monitoring.3,ifms-1.5.1,ifms-1.5.1-dict-range.1,ifms-1.5.1-forecast-dates.1,ifms-1.5.1-forecast-dates.2,ifms-1.5.1-irving-targeting.1,ifms-1.5.1-mood6.1,ifms-1.5.1-mood6.2,ifms-1.5.1-pubmatic-samples.1,ifms-1.5.1-pubmatic.1,ifms-1.5.1-pubmatic.2,ifms-1.5.1-pubmatic.3,ifms-1.5.1-pubmatic.4,ifms-1.5.1-pubmatic.5,ifms-1.5.1-pubmatic.6,ifms-1.5.1-pubmatic.7,ifms-1.5.1-pubmatic.8,ifms-1.5.1-pubmatic.9,ifms-1.5.1-pubmatic.10,ifms-1.5.1-pubmatic.11,ifms-1.5.1-pubmatic.12,ifms-1.5.1-pubmatic.13,ifms-1.5.1-pubmatic.14",
         "name" : "ifms_packages",
         "certname" : "eu-smr-dev-01.inventale.com"
       }, {
         "value" : "ifms-js-1.0.105-forecast-result.4,ifms-js-1.0.105-ym-report-dates.1,ifms-js-1.0.106,ifms-js-1.0.106-targeting-list.3,ifms-js-1.0.106-wrong-product-label.1,ifms-js-1.0.106-ym-datepicker.1,ifms-js-1.0.107,ifms-js-1.0.107-autotest-and-ui-fixes.1,ifms-js-1.0.107-download-handler.1,ifms-js-1.0.107-external-events-float-number.1,ifms-js-1.0.107-sections-hash.1,ifms-js-1.0.107-small-fixes.1,ifms-js-1.0.108,ifms-js-1.0.108-calendar-on-scrolling.1,ifms-js-1.0.108-dsp-changes.1,ifms-js-1.0.108-expired-session-message.1,ifms-js-1.0.108-external-events-float-number.2,ifms-js-1.0.108-forecast-result.6,ifms-js-1.0.108-forecast-result.7,ifms-js-1.0.108-targeting-list.4,ifms-js-1.0.108-targeting-list.5,ifms-js-1.0.109,ifms-js-1.0.109-forecast-result.5,ifms-js-1.0.109-forecast-result.8,ifms-js-1.0.109-forecast-result.9,ifms-js-1.0.109-forecast-result.10,ifms-js-1.0.109-monitoring-width.1,ifms-js-1.0.109-ym-i18n-fix.1,ifms-js-1.0.110-build-prod.1,ifms-js-1.0.110-campaign-type-in-forecast-result.1,ifms-js-1.0.110-cross-filter-mixpanel-tracking.1,ifms-js-1.0.110-forecast-zero-results-bugs.1,ifms-js-1.0.110-forecast-zero-results-bugs.2,ifms-js-1.0.110-language-in-ui-mixpanel-property.1,ifms-js-1.0.110-language-in-ui-mixpanel-property.2,ifms-js-1.0.110-monitoring-new-columns.1,ifms-js-1.0.110-monitoring-new-columns.2,ifms-js-1.0.110-pubmatic-implementation.1,ifms-js-1.0.110-pubmatic-implementation.2,ifms-js-1.0.110-request-dates.1,ifms-js-1.0.110-request-dates.2,ifms-js-1.0.110-request-dates.3,ifms-js-1.0.110-ym-notifications.1,ifms-js-1.0.110-ym-tooltip-hide-fix.1,ifms-js-1.1.1,ifms-js-1.1.1-pubmatic-implementation.3,ifms-js-1.1.1-pubmatic-implementation.4,ifms-js-1.1.1-pubmatic-implementation.5,ifms-js-1.3.19-campaignautomodified.1,ifms-js-1.3.19-rickdefaultcamp.1",
         "name" : "ifmsjs_packages",
         "certname" : "eu-smr-dev-01.inventale.com"
       }, {
         "value" : "sda1",
         "name" : "hdd_list",
         "certname" : "eu-smr-dev-01.inventale.com"
       }, {
         "value" : "6.28",
         "name" : "load_1m",
         "certname" : "eu-smr-dev-01.inventale.com"}]
"""

b = json.loads(a)
print('stop')
