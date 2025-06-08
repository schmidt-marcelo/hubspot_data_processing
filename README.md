# Hubspot Data Integration

This is a sample app to exercise the use of nodejs Hubspot api client. I've used what's in the original folder as a base and perform some optimizations on the solution folder. The original implementation consumes company and contact data, and takes more than 10min to finish. 
The solution implementation, on the other hand, consumes company, contact and meetings data and takes 3 to 4 seconds to finish. 

## Architecture optimizations:

1) Isolate the calls to hubspot api using decorators so that the code doesn't depend directly on the client api, and workers don't need to worry about refreshing tokens and pagination for instance;
2) Implement Open-Closed Principle, having each process in a separated worker, so that new workers can be implemented and make use of the same advantages;
3) Separate the persistence of the domain in another file

## Performance optimizations:

1) Remove all the retry mechanism and use the native configuration of hubspot client api;
2) Paralelize the data gathering processes using ```Promise.all```;
3) Remove Domain's data traversing to get account data;
4) Make processes return the actions array so that they are pushed to queue all at once for each account;
5) Domain is saved only once at the end of the whole process;
6) Remove the load of the server.js since it's not used;
7) Make use of paralelization provided by ```async``` library;
8) Removed ```apikey``` and ```hubId``` from logging entries once it's a security breach;
9) Make use of local caching of contacts data to avoid unnecessary calls to hubspot api;

## Code quality and readability

Most of the above optimizations already adresses the code readability, in my opinion. About code quality, I would implement tests: unit tests (+ mutation), integration tests and e2e tests.

## Execution

### Before

```
connected to database
1749411560581: start processing pulling data from Hubspot
1749411561511: start processing start processing account 26980676
1749411562110: start processing processContacts
1749411874370 fetch contact batch
1749411874750: finish processing processContacts in 5:13 minutes
1749411874751: start processing processCompanies
1749412187223 fetch company batch
1749412187226: finish processing processCompanies in 5:12 minutes
1749412187227 process companies
1749412187227 drain queue
1749412187228: finish processing account in 10:27 minutes
1749412187228: finish processing pulling data from Hubspot in 10:27 minutes
```

### After

````
1749412972208: start processing pulling data from HubSpot
1749412973144: start processing account
1749412974146: start processing contacts
1749412974175: start processing companies
1749412974190: start processing meetings
1749412974548: finish processing meetings in 0:00 minutes
1749412974860: finish processing contacts in 0:01 minutes
1749412976506: finish processing companies in 0:02 minutes
1749412976509: processed all data
1749412976521: Database insert.
1749412976521: drain queue
1749412976521: finish processing account. 459 actions stored: 9 contacts, 417 companies, 33 meetings.
1749412976521: finish processing pulling data from HubSpot in 0:04 minutes
