-- set up some sample cash flows
declare @cashflows table (
	ToDate date not null primary key,
	CashFlow numeric(19,2) not null
);

insert into @cashflows values
('2015-06-30', -1000000),
('2019-03-15',   800000),
('2023-09-30',  1200000);

-- set variables
declare @from_date date = (select min(ToDate) from @cashflows);
declare @guess float = 0.1;  -- first guess as a starting point for the iterative process

-- define recursive tree, calculating Net Present Value of cash flows with each successive IRR estimate, and estimating the new IRR.
with iter_data as (

	-- anchor member, with the first guess as IRR.
	select
		convert(int, 0) as IterNr,  -- iteration counter
		convert(varchar(50), 'Starting') as IterStatus,
		convert(float, 0) as IRR_Previous,  -- 0 % as "previous IRR"
		convert(float, @guess) as IRR,  -- starting from guess as IRR
		convert(float, sum(cf.CashFlow)) as NPV_Previous,
		convert(float, sum(cf.CashFlow / power(1e0 + @guess, cft.CashFlowTerm))) as NPV,  -- net present value with initial guess
		convert(smallint, 1) as row_nr
	from
		@cashflows cf
		cross apply (select convert(float, datediff(day, @from_date, cf.ToDate)) / 365 as CashFlowTerm) cft  -- calculate cash flow term in years

	union all

	-- recursive member, using iter_data itself in from clause.
	-- combining previous round IRR estimates with cash flows to calculate new IRRs and NPVs.
	select
		iter_set.IterNr,
		q.IterStatus,
		iter_set.IRR_Previous,	
		iter_set.IRR,
		iter_set.NPV_Previous,
		iter_set.NPV,
		iter_set.row_nr
	from (
		select
			id.IterNr + 1 as IterNr,
			id.IRR as IRR_Previous,
			n.IRR,  -- the new IRR iteration result  
			id.NPV as NPV_Previous,
			case 
				when n.IRR <= -0.99 then null
				else sum(convert(float, cf.CashFlow) / power(1e0 + n.IRR, cft.CashFlowTerm)) over ()  -- use window function to sum over all cash flows
			end as NPV,  -- net present value with new IRR
			convert(smallint, row_number() over (order by (select null))) as row_nr  -- row number needed to pick just one of [count of cashflows] identical rows
		from
			iter_data id
			cross join @cashflows cf
			cross apply (select convert(float, datediff(day, @from_date, cf.ToDate)) / 365 as CashFlowTerm) cft  -- calculate cash flow term in years
			cross apply (
				-- iterate towards the solution by estimating the new IRR
				select
					case 
						when id.NPV - id.NPV_Previous = 0 then id.IRR  -- no change in NPV, keep the result
						when id.IRR - (id.IRR - id.IRR_Previous) * id.NPV / (id.NPV - id.NPV_Previous) < -0.99 then -0.99  -- avoid <= -100 % IRR and the resulting error
						else id.IRR - (id.IRR - id.IRR_Previous) * id.NPV / (id.NPV - id.NPV_Previous)
					end as IRR
			) n

		where
			-- continue until result is final or an error status encountered
			id.IterStatus in ('Starting', 'Iterating')

		) iter_set

		cross apply (
			-- check if this was the final IRR - either accurate enough, maximum number of iterations reached, or no result
			select
				convert(varchar(50), case
					when abs(iter_set.NPV) <= 0.01 then 'Final'     -- break loop when NPV close enough to zero
					when iter_set.IterNr >= 20 then 'Error: max recursive loops'
					when iter_set.IRR > 100 then 'Error: IRR > 10,000 %'
					when iter_set.IRR <= -0.99 then 'Error: IRR <= -99 %'
					else 'Iterating'
				end) as IterStatus
		) q

	where
		iter_set.row_nr = 1

)

-- see iteration sequence
select * from iter_data order by IterNr;
-- get just the final result (comment out the above)
--select IRR from iter_data where IterStatus not in ('Starting', 'Iterating');

