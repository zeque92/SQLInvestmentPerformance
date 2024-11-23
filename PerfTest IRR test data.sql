
-- set up a new portfolio (not required, but not to mix up with other data)
insert into PerfTest.Portfolio values
(3, 'Private Assets','Private Assets Portfolio', 'EUR')

-- set up two Private Equity Fund instruments
insert into PerfTest.Instrument values
(58, 'MMBO PE Fund', 'Middle Market Buyout Private Equity Fund', 'USD', 'US', 'North America', 'Private Equity', 'Private Equity'),
(59, 'VC PE Fund', 'Venture Capital Private Equity Fund', 'EUR', 'DE', 'Europe', 'Private Equity', 'Private Equity')

-- add a filtered index to support Private Equity asset class queries
create index Instrument_IX#InstrumentIK$Private_Equity on PerfTest.Instrument (InstrumentIK asc) 
include (ShortName, LongName, Currency) where AssetClass = 'Private Equity';

-- add Private Equity Fund transactions. Commitment is reflected by Units, and cash flows by CurrencyValue
insert into PerfTest.Transact values
(107,'2015-05-29',3,58,52,5000000,'USD',0, 'Commitment'),
(108,'2015-07-24',3,58,52,0,'USD',  800000,'Capital Call'),
(109,'2015-11-17',3,58,52,0,'USD',  450000,'Capital Call'),
(110,'2016-02-19',3,58,52,0,'USD',  320000,'Capital Call'),
(111,'2016-03-15',3,58,52,0,'USD',  -24500,'Profit Distribution'),
(112,'2016-06-27',3,58,52,0,'USD',  580000,'Capital Call'),
(113,'2016-10-24',3,58,52,0,'USD',  -37000,'Profit Distribution'),
(114,'2017-01-05',3,58,52,0,'USD',  440000,'Capital Call'),
(115,'2017-01-20',3,58,52,0,'USD',  375000,'Capital Call'),
(116,'2017-04-18',3,58,52,0,'USD', -280000,'Capital Return'),
(117,'2017-06-30',3,58,52,0,'USD',  720000,'Capital Call'),
(118,'2017-08-18',3,58,52,0,'USD',  -46000,'Profit Distribution'),
(119,'2017-10-24',3,58,52,0,'USD',  428000,'Capital Call'),
(120,'2018-03-16',3,58,52,0,'USD',  -34000,'Profit Distribution'),
(121,'2018-05-25',3,58,52,0,'USD',  530000,'Capital Call'),
(122,'2018-10-05',3,58,52,0,'USD',  -27000,'Profit Distribution'),
(123,'2018-10-05',3,58,52,0,'USD', -320000,'Capital Return'),
(124,'2018-12-04',3,58,52,0,'USD',  295000,'Capital Call'),
(125,'2019-05-28',3,58,52,0,'USD', -320000,'Capital Return'),
(126,'2019-05-28',3,58,52,0,'USD',  -27000,'Profit Distribution'),
(127,'2020-03-03',3,58,52,0,'USD', -235000,'Capital Return'),
(128,'2020-03-03',3,58,52,0,'USD',  -40000,'Profit Distribution'),
(129,'2020-11-18',3,58,52,0,'USD', -740000,'Capital Return'),
(130,'2020-11-18',3,58,52,0,'USD', -349000,'Profit Distribution'),
(131,'2021-05-07',3,58,52,0,'USD', -190000,'Capital Return'),
(132,'2021-05-07',3,58,52,0,'USD', -230000,'Profit Distribution'),
(133,'2022-02-08',3,58,52,0,'USD', -760000,'Capital Return'),
(134,'2022-02-08',3,58,52,0,'USD', -301500,'Profit Distribution'),
(135,'2022-10-18',3,58,52,0,'USD', -620000,'Capital Return'),
(136,'2022-10-18',3,58,52,0,'USD', -364000,'Profit Distribution'),
(137,'2023-06-15',3,58,52,0,'USD', -790000,'Capital Return'),
(138,'2023-06-15',3,58,52,0,'USD', -595000,'Profit Distribution'),
(139,'2024-09-27',3,58,52,0,'USD', -450000,'Capital Return'),
(140,'2024-09-30',3,58,52,-5000000,'USD',0,'Fund Closed'),
(141,'2016-02-19',3,59,51,8000000,'EUR',0,'Commitment'),
(142,'2016-03-15',3,59,51,0,'EUR',2000000,'Capital Call'),
(143,'2016-06-27',3,59,51,0,'EUR',1250000,'Capital Call'),
(144,'2016-10-24',3,59,51,0,'EUR',2625000,'Capital Call'),
(145,'2017-01-05',3,59,51,0,'EUR',375000,'Capital Call'),
(146,'2017-01-20',3,59,51,0,'EUR',-1050000,'Profit Distribution'),
(147,'2017-04-18',3,59,51,0,'EUR',900000,'Capital Call'),
(148,'2017-06-30',3,59,51,0,'EUR',-360000,'Profit Distribution'),
(149,'2017-08-18',3,59,51,0,'EUR',-245000,'Profit Distribution'),
(150,'2017-10-24',3,59,51,0,'EUR',-440000,'Profit Distribution'),
(151,'2018-03-16',3,59,51,0,'EUR',800000,'Capital Call'),
(152,'2018-05-25',3,59,51,0,'EUR',-330000,'Profit Distribution'),
(153,'2018-10-05',3,59,51,0,'EUR',-775000,'Capital Return'),
(154,'2018-10-05',3,59,51,0,'EUR',-400000,'Profit Distribution'),
(155,'2019-05-28',3,59,51,0,'EUR',-1250000,'Capital Return'),
(156,'2019-05-28',3,59,51,0,'EUR',-180000,'Profit Distribution'),
(157,'2020-11-18',3,59,51,0,'EUR',-1080000,'Capital Return'),
(158,'2020-11-18',3,59,51,0,'EUR',-620000,'Profit Distribution'),
(159,'2021-05-07',3,59,51,0,'EUR',-2450000,'Capital Return'),
(160,'2021-05-07',3,59,51,0,'EUR',-790000,'Profit Distribution'),
(161,'2022-02-08',3,59,51,0,'EUR',-1800000,'Capital Return'),
(162,'2022-02-08',3,59,51,0,'EUR',-450000,'Profit Distribution'),
(163,'2022-10-18',3,59,51,0,'EUR',-400000,'Capital Return'),
(164,'2022-10-18',3,59,51,0,'EUR',-160000,'Profit Distribution'),
(165,'2022-11-30',3,59,51,-8000000,'EUR',0,'Fund Closed')


-- add Private Equity fund valuations. Assuming semiannual NAVs, per "unit of commitment".
insert into PerfTest.Price values
('2015-05-29',58,'USD',0.000000),
('2015-06-30',58,'USD',0.000000),
('2015-12-31',58,'USD',0.260000),
('2016-06-30',58,'USD',0.445000),
('2016-12-30',58,'USD',0.442000),
('2017-06-30',58,'USD',0.696000),
('2017-12-29',58,'USD',0.810000),
('2018-06-29',58,'USD',0.950000),
('2018-12-31',58,'USD',1.004000),
('2019-06-28',58,'USD',0.990000),
('2019-12-31',58,'USD',0.960000),
('2020-06-30',58,'USD',0.940000),
('2020-12-31',58,'USD',0.810000),
('2021-06-30',58,'USD',0.712000),
('2021-12-31',58,'USD',0.776000),
('2022-06-30',58,'USD',0.548000),
('2022-12-30',58,'USD',0.398000),
('2023-06-30',58,'USD',0.128000),
('2023-12-29',58,'USD',0.123000),
('2024-06-28',58,'USD',0.100000),
('2024-09-30',58,'USD',0.000000),
('2016-02-19',59,'EUR',0.000000),
('2016-06-30',59,'EUR',0.418750),
('2016-12-30',59,'EUR',0.881250),
('2017-06-30',59,'EUR',0.843750),
('2017-12-29',59,'EUR',0.818750),
('2018-06-29',59,'EUR',0.928125),
('2018-12-31',59,'EUR',0.887500),
('2019-06-28',59,'EUR',0.718750),
('2019-12-31',59,'EUR',0.752500),
('2020-06-30',59,'EUR',0.783750),
('2020-12-31',59,'EUR',0.550000),
('2021-06-30',59,'EUR',0.277500),
('2021-12-31',59,'EUR',0.300000),
('2022-06-30',59,'EUR',0.075000),
('2022-11-30',59,'EUR',0.000000)


-- add FX rates on each cash flow and valuation date.
insert into PerfTest.FXRate values
('2015-05-29','EUR','USD',1.0970),
('2015-06-30','EUR','USD',1.1189),
('2015-07-24','EUR','USD',1.0939),
('2015-11-17','EUR','USD',1.0670),
('2015-12-31','EUR','USD',1.0887),
('2016-02-19','EUR','USD',1.1096),
('2016-03-15','EUR','USD',1.1109),
('2016-06-27','EUR','USD',1.0998),
('2016-06-30','EUR','USD',1.1102),
('2016-10-24','EUR','USD',1.0891),
('2016-12-30','EUR','USD',1.0541),
('2017-01-05','EUR','USD',1.0501),
('2017-01-20','EUR','USD',1.0632),
('2017-04-18','EUR','USD',1.0682),
('2017-06-30','EUR','USD',1.1412),
('2017-08-18','EUR','USD',1.1740),
('2017-10-24','EUR','USD',1.1761),
('2017-12-29','EUR','USD',1.1993),
('2018-03-16','EUR','USD',1.2301),
('2018-05-25','EUR','USD',1.1675),
('2018-06-29','EUR','USD',1.1658),
('2018-10-05','EUR','USD',1.1506),
('2018-12-04','EUR','USD',1.1409),
('2018-12-31','EUR','USD',1.1450),
('2019-05-28','EUR','USD',1.1192),
('2019-06-28','EUR','USD',1.1380),
('2019-12-31','EUR','USD',1.1234),
('2020-03-03','EUR','USD',1.1117),
('2020-06-30','EUR','USD',1.1198),
('2020-11-18','EUR','USD',1.1868),
('2020-12-31','EUR','USD',1.2271),
('2021-05-07','EUR','USD',1.2059),
('2021-06-30','EUR','USD',1.1884),
('2021-12-31','EUR','USD',1.1326),
('2022-02-08','EUR','USD',1.1408),
('2022-06-30','EUR','USD',1.0387),
('2022-10-18','EUR','USD',0.9835),
('2022-12-30','EUR','USD',1.0666),
('2023-06-15','EUR','USD',1.0819),
('2023-06-30','EUR','USD',1.0866),
('2023-12-29','EUR','USD',1.1050),
('2024-06-28','EUR','USD',1.0705),
('2024-09-27','EUR','USD',1.1158),
('2024-09-30','EUR','USD',1.1196)
