SELECT 
	c.relname as relname,
	rn.nspname as relnspname,
	s.staattnum as staattnum,
	s.starelkind as starelkind, 
	t.typname as typname, 
	tn.nspname as typnspname, 
	s.stainherit as stainherit,
	s.stanullfrac as stanullfrac,
	s.stawidth as stawidth,
	s.stadistinct as stadistinct,
	s.stadndistinct as stadndistinct,
	s.stakind1 as stakind1,
	s.stakind2 as stakind2,
	s.stakind3 as stakind3,
	s.stakind4 as stakind4,
	s.stakind5 as stakind5,
	s.staop1 as staop1,
	s.staop2 as staop2,
	s.staop3 as staop3,
	s.staop4 as staop4,
	s.staop5 as staop5,
	stanumbers1::text,
	stanumbers2::text,
	stanumbers3::text,
	stanumbers4::text,
	stanumbers5::text,
	stavalues1::text,
	stavalues2::text,
	stavalues3::text,
	stavalues4::text,
	stavalues5::text
FROM pg_statistic s JOIN pg_class c ON(c.oid = s.starelid AND c.relnamespace <> 11)
JOIN pg_attribute a ON (a.attrelid = s.starelid AND a.attnum = s.staattnum) 
JOIN pg_namespace rn ON(rn.oid = c.relnamespace AND rn.nspname NOT IN('pg_toast', 'pg_catalog', 'information_schema'))
JOIN pg_type t ON (t.oid = a.atttypid)
JOIN pg_namespace tn ON(tn.oid = t.typnamespace)
