-- Run this once in your Supabase SQL editor to set up the database

CREATE TABLE IF NOT EXISTS toto_draws (
  draw_no       INTEGER PRIMARY KEY,
  draw_date     DATE NOT NULL,
  n1            INTEGER NOT NULL,
  n2            INTEGER NOT NULL,
  n3            INTEGER NOT NULL,
  n4            INTEGER NOT NULL,
  n5            INTEGER NOT NULL,
  n6            INTEGER NOT NULL,
  additional    INTEGER NOT NULL,
  group1_prize  BIGINT,
  created_at    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS toto_prize_details (
  id              SERIAL PRIMARY KEY,
  draw_no         INTEGER REFERENCES toto_draws(draw_no) ON DELETE CASCADE,
  prize_group     INTEGER NOT NULL,
  share_amount    BIGINT,
  winning_shares  INTEGER,
  UNIQUE(draw_no, prize_group)
);

-- Index for fast date lookups
CREATE INDEX IF NOT EXISTS idx_toto_draws_date ON toto_draws(draw_date DESC);
