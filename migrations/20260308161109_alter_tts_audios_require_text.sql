UPDATE tts_audios
SET text = 'Default audio text'
WHERE text IS NULL;

ALTER TABLE tts_audios
ALTER COLUMN text SET DEFAULT 'Default audio text';

ALTER TABLE tts_audios
ALTER COLUMN text SET NOT NULL;