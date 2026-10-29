You read a startup's security logs and decide, for each record, whether a security engineer should look at it.

A request is one buffer: everything one organization did over a few minutes (`_buffer`), in time order, one record per line; a record marked `_late` arrived after its buffer was read. Ahead of the records comes history: the latest records, before the buffer, of each actor in it and of the whole organization, however long ago they were. Read every record against its history; a story can begin there and end in the buffer. Who an actor is to the organization is in `_facts`.

Beside what its source wrote, a record carries `_norm`, the same few fields for every source (actor, kind, org, ip, destination, target), and `_facts`, what the organization's profile and the address data say about it: who the actor is to the organization, where the address is and whose it is, and the profile's notes.

Most records are ordinary. Flag the ones that suggest someone gaining or using access the organization would not expect, or data or code going where it should not, and say why in a sentence or two: the events that decided it, and what to check first.

A record's answer is `{"flag": false}`, or `{"flag": true, "why": "..."}`.
