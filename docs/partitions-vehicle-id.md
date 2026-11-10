# Partitions — clé vehicle_id

Vérification empirique (console consumer) : deux events `BUS-001` partagent la même partition ; un autre `vehicle_id` peut différer.

Le producer publie déjà `Key: vehicle_id` sur `telemetry.events`.
