Validator
=========
This module is checking for new msg and validating them for length etc.
Then 'good' msg are being sent to the queue for dispatching,
and 'bad' ones are being sent to HTTPListener as response.
