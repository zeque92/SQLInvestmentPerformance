create or alter view [PerfTest].[vMarketValueAndCashFlow] as

--------------------------------------------------------------------------------------------------------------------------
-- This view calculates daily number of units, market values, and cashflows from transactions, prices and FX rates.
-- Market values and cashflows are reported in Instrument Currency (IC) and Portfolio Currency (PC).
-- Note: assumes prices and fx rates exist for all calendar dates (when there is a holding or cashflow in the respective instrument).
--
-- 2023-12-16   Kalle Saariaho     Initial release
--------------------------------------------------------------------------------------------------------------------------

	with trans as (
		-- portfolio instrument transactions
		select
			PortfolioIK,
			InstrumentIK,
			TradeDate,
			Currency,
			Units,
			CurrencyValue
		from
			PerfTest.Transact
		-- add transaction based changes on cash accounts. Use the negative CurrencyValue as change in units & value (note: assumes transaction currency = cash instrument currency).
		union all
		select 
			PortfolioIK,
			CashInstrumentIK as InstrumentIK,
			TradeDate,
			Currency,
			-CurrencyValue as Units,
			-CurrencyValue as CurrencyValue
		from
			PerfTest.Transact
		where
			CashInstrumentIK is not null 
		),

	units as (
		-- sum up all transactions above until each ToDate
		select
			t.PortfolioIK,
			t.InstrumentIK,
			t.Currency,
			c.ToDate,
			c.PrevToDate,
			sum(t.Units) as Units,
			-- summing up transaction where TradeDate > calendar PrevToDate (but <= calendar ToDate, for each calendar ToDate)
			--   will also include transactions where TradeDate is not a calendar date
			sum(case when t.TradeDate > c.PrevToDate or c.PrevToDate is null then t.CurrencyValue else 0 end) as CashFlowTC
		from
			PerfTest.Calendar c
			inner join trans t on t.TradeDate <= c.TODATE
		group by
			t.PortfolioIK,
			t.InstrumentIK,
			t.Currency,
			c.ToDate,
			c.PrevToDate
		having
			-- include data only if there are units or a cashflow
			sum(t.Units) <> 0
			or sum(case when t.TradeDate > c.PrevToDate or c.PrevToDate is null then t.CurrencyValue else 0 end) <> 0
		)

	-- add valuation data (prices and fx rates) and lagged market values. [Possible also to add total portfolio daily market value for easier weight calculations.]
	select
		u.PortfolioIK,
		por.ShortName as PortfolioShortName,
		por.Currency as PortfolioCurrency,
		u.InstrumentIK,
		i.ShortName,
		i.LongName,
		i.AssetClass,
		i.Region,
		i.Country,
		i.Currency,
		i.Sector,
		u.ToDate,
		u.Units,
		u.Currency as TransCurrency,
		p.PriceCurrency,
		p.Price,
		v.FXRatePC,
		v.MarketValueIC,
		v.CashFlowIC,
		case
			when lag(u.ToDate, 1) over (partition by u.PortfolioIK, u.InstrumentIK, u.Currency order by u.ToDate) = u.PrevToDate
			then lag(v.MarketValueIC, 1, 0.00) over (partition by u.PortfolioIK, u.InstrumentIK, u.Currency order by u.ToDate) 
			else 0.00
		end as lag_MarketValueIC,
		v.MarketValuePC,
		v.CashFlowPC,
		case
			when lag(u.ToDate, 1) over (partition by u.PortfolioIK, u.InstrumentIK, u.Currency order by u.ToDate) = u.PrevToDate
			then lag(v.MarketValuePC, 1, 0.00) over (partition by u.PortfolioIK, u.InstrumentIK, u.Currency order by u.ToDate) 
			else 0.00
		end as lag_MarketValuePC

	from
		PerfTest.Portfolio por
		inner join units u on u.PortfolioIK = por.PortfolioIK
		inner join PerfTest.Instrument i on i.InstrumentIK = u.InstrumentIK
		left outer join PerfTest.Price p on p.InstrumentIK = u.InstrumentIK and p.PriceDate = u.ToDate
		-- price FX rate to EUR
		left outer join PerfTest.FXRate fxp on fxp.Currency = 'EUR' and fxp.PriceCurrency = p.PriceCurrency and fxp.PriceDate = u.ToDate and p.PriceCurrency <> 'EUR'
		-- transaction (cashflow) FX rate to EUR
		left outer join PerfTest.FXRate fxt on fxt.Currency = 'EUR' and fxt.PriceCurrency = u.Currency and fxt.PriceDate = u.ToDate and fxt.PriceCurrency <> 'EUR'
		-- instrument FX rate to EUR
		left outer join PerfTest.FXRate fxi on fxi.Currency = 'EUR' and fxi.PriceCurrency = i.Currency and fxi.PriceDate = u.ToDate and i.Currency <> 'EUR'
		-- portfolio FX rate to EUR
		left outer join PerfTest.FXRate fxpor on fxpor.Currency = 'EUR' and fxpor.PriceCurrency = por.Currency and fxpor.PriceDate = u.ToDate and por.Currency <> 'EUR'
		cross apply (
			-- using cross apply to simplify fx rate handling, as for base currency EUR there are no quotes in data
			select
				case when p.PriceCurrency = 'EUR' then 1.00000000 else fxp.FXRate   end as FXRatePrice,
				case when u.Currency      = 'EUR' then 1.00000000 else fxt.FXRate   end as FXRateTrans,
				case when i.Currency      = 'EUR' then 1.00000000 else fxi.FXRate   end as FXRateInstrument,
				case when por.Currency    = 'EUR' then 1.00000000 else fxpor.FXRate end as FXRatePortfolio
		) fx
		cross apply (
			-- using another cross apply to calculate market values and cash flows in IC and PC
			-- converting precision to "currency" i.e. two decimals - a convention
			select
				convert(numeric(19,8), fx.FXRatePrice / fx.FXRatePortfolio) as FXRatePC,
				convert(numeric(19,2), u.Units * p.Price / (fx.FXRatePrice / fx.FXRateInstrument)) as MarketValueIC,
				convert(numeric(19,2), u.CashFlowTC / (fx.FXRateTrans / fx.FXRateInstrument)) as CashFlowIC,
				convert(numeric(19,2), u.Units * p.Price / (fx.FXRatePrice / fx.FXRatePortfolio)) as MarketValuePC,
				convert(numeric(19,2), u.CashFlowTC / (fx.FXRateTrans / fx.FXRatePortfolio)) as CashFlowPC
		) v;


GO


