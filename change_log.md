### Version 0.5.0
- The collection pipeline is ready for initial tests, including:
  - Metadata management
  - A-synchronized requests to the API
  - Preprocessing
  - Validation
  - Uploading to cloud storage
- FastAPI route to initialize the collection process

### Version 0.5.1
- Erred API requests are saved to a file in the storage

### Version 0.5.2
- minor bugs fix

### Version 1.0.0
- deprecated the use of rapidapi as midiator
- deprecated the route collect, the collection pipeline is now initiated when the application is running, unless previously completed.
- the update pipeline updates the data from the last 12 months.
- a multithreading approach is used to speed up the collection process.
- requests to the API use proxy to prevent detection.
- erred requests are retied n times.

### Roadmap
#### Versions:

#### Features
