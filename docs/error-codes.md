# Error codes

Every coded gRPC error from the collab service carries an `ErrorInfo` with the symbol as
its reason, the domain `collab` and the code in `codeNum`. Only user-safe messages reach
the caller; every other code is sent as `Code N: Internal Error`.

| Code | Symbol | Area | Cause | User-safe |
| --- | --- | --- | --- | --- |
| 8500 | `INTERNAL` | collab | an uncoded failure inside the collab service | no |
| 8501 | `POLICY_ID_REQUIRED` | request | the request has no policy_id; the gateway always sets it, so this is a client bug | no |
| 8502 | `DRAFT_ID_REQUIRED` | request | the request has no draft_id; the gateway always sets it, so this is a client bug | no |
| 8503 | `VERSION_NUMBER_INVALID` | publish | the published version number is not positive, so nothing was published (0 is core's draft sentinel) | no |
| 8504 | `ACTOR_REQUIRED` | request | the call carries no go-grpc-actor actor, so there is nobody to mint a token for or attribute the action to | no |
| 8505 | `DRAFT_EDIT_FORBIDDEN` | token | the caller neither owns the policy nor holds an author grant in its home category or an ancestor | yes |
| 8506 | `ROOM_POLICY_MISMATCH` | room | the live room for the draft belongs to a different policy than the request names, so the room is left alone | no |
| 8507 | `FLUSH_REJECTED` | flush | core refused the live room's newest checkpoint (reason and detail in the metadata), so core doesn't hold the room's content | yes |
| 8508 | `FLUSH_UNAVAILABLE` | flush | core couldn't be reached to save the live room's newest checkpoint; it stays queued for a retry | yes |
| 8509 | `FLUSH_TIMEOUT` | flush | core didn't answer the live room's checkpoint in time; it stays queued for a retry | yes |
| 8510 | `POLICY_NOT_FOUND` | token | core has no policy with the requested id | yes |
| 8511 | `EDIT_CHECK_UNAVAILABLE` | token | core or identity couldn't be reached to decide whether the caller may edit the draft | yes |
