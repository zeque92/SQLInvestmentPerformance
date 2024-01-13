select
	p.*
from (
	select 
		s.StructureIK,
		concat(s.Level1, ' - ', s.Level2, ' - ', s.Level3, ' - ', s.Level4) as Levels,
		p.LevelNr
	from 
		PerfTest.vPerformanceTimeSeries p
		inner join PerfTest.Structure s on s.StructureIK = p.StructureIK
	where 
		p.PortfolioIK = 1 
		and p.ToDate = '2023-10-16' 
) a
pivot (
	count(LevelNr)
	for LevelNr in ([0], [1], [2], [3], [4])
) p;