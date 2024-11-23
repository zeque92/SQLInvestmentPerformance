
CREATE OR ALTER   view [PerfTest].[vPEFundMarketValueAndCashFlowByCCY] as

--------------------------------------------------------------------------------------------------------------------------
-- This view calculates number of units (=commitment), market values, and cashflows from transactions, prices and FX rates
--   for Private Equity AssetClass instruments. Respective cash accounts and other instruments are ignored.
-- Market values and cashflows are reported in Instrument Currency (IC) and Portfolio Currency (PC).
-- Market values are based on last NAV Price and current Units. A CashFlowAdjustment is reported, reflecting any added capital or distribution after latest NAV.
-- Additionally, DPI (Distributions per Paid-in Capital), RVPI (Residual Value per Paid-in Capital, and TVPI (Total Value per Paid-in Capital) as reported.
-- Note: reports only dates when there is a transaction or valuation (price). 
-- Note: assumes FX Rates can be found on all transaction and valuation dates for required currencies.
--
-- 2024-11-16   Kalle Saariaho     Initial release
--------------------------------------------------------------------------------------------------------------------------

with pf_instr as (
	-- get the portfolios and instruments, limit to 'Private Equity' AssetClass only.
	select distinct
		t.PortfolioIK,
		i.InstrumentIK,
		i.ShortName,
		i.LongName,
		i.Currency as InstrumentCurrency
	from
		PerfTest.Transact t
		inner join PerfTest.Instrument i on i.InstrumentIK = t.InstrumentIK
	where
		i.AssetClass = 'Private Equity'
	),
	
-- combine transactions with valuations to get a full time series of calculation dates.
trans_valuations as (
	-- get all transactions, add Price prevailing on TradeDate.
	select
		pin.PortfolioIK,
		pin.InstrumentIK,
		pin.ShortName,
		pin.LongName,
		pin.InstrumentCurrency,
		t.Currency as TransCurrency,
		t.TradeDate as ToDate,
		t.Units,
		t.CurrencyValue,
		pr.PriceDate,
		pr.PriceCurrency,
		pr.Price
	from
		pf_instr pin
		inner join PerfTest.Transact t on t.PortfolioIK = pin.PortfolioIK and t.InstrumentIK = pin.InstrumentIK
		cross apply (
			-- get the latest Price. 
			select top 1
				p.PriceDate,
				p.PriceCurrency,
				p.Price
			from
				PerfTest.Price p 
			where 
				p.InstrumentIK = pin.InstrumentIK 
				and p.PriceDate <= t.TradeDate
			order by
				p.PriceDate desc
		) pr

	union all
	-- add all valuation date Prices.
	select
		pin.PortfolioIK,
		pin.InstrumentIK,
		pin.ShortName,
		pin.LongName,
		pin.InstrumentCurrency,
		null as TransCurrency,
		p.PriceDate as ToDate,
		0 as Units,
		0 as CurrencyValue,
		p.PriceDate,
		p.PriceCurrency,
		p.Price
	from
		pf_instr pin
		inner join PerfTest.Price p on p.InstrumentIK = pin.InstrumentIK
	where
		-- transaction TradeDates already accounted for in the first select, so exclude them here.
		not exists (select 1 from PerfTest.Transact t where t.PortfolioIK = pin.PortfolioIK and t.InstrumentIK = pin.InstrumentIK and t.TradeDate = p.PriceDate)
	),

dates as (
	-- add fx rates and portfolio data, sum up to one row per date and CalculationCurrency.
	select 
		tv.PortfolioIK,
		por.ShortName as PortfolioShortName,
		por.LongName as PortfolioLongName,
		por.Currency as PortfolioCurrency,
		tv.InstrumentIK,
		tv.ShortName,
		tv.LongName,
		tv.InstrumentCurrency,
		tv.ToDate,
		tv.PriceDate,
		tv.PriceCurrency,
		tv.Price as TransPrice,
		sum(tv.Units) as Units,
		ccy.CalculationCurrencyType,
		ccy.CalculationCurrency,
		ccy.FXRateTrans,
		ccy.FXRatePrice,
		max(convert(float, tv.Price) / ccy.FXRatePrice) as Price,
		sum(convert(float, tv.CurrencyValue) / ccy.FXRateTrans) as CashFlow,
		sum(convert(float, case when tv.CurrencyValue > 0 then  tv.CurrencyValue else 0 end) / ccy.FXRateTrans) as CapitalCall,
		sum(convert(float, case when tv.CurrencyValue < 0 then -tv.CurrencyValue else 0 end) / ccy.FXRateTrans) as Distributions
	from 
		trans_valuations tv
		inner join PerfTest.Portfolio por on por.PortfolioIK = tv.PortfolioIK
		-- price FX rate to EUR
		left outer join PerfTest.FXRate fxp on fxp.Currency = 'EUR' and fxp.PriceCurrency = tv.PriceCurrency and fxp.PriceDate = tv.ToDate and tv.PriceCurrency <> 'EUR'
		-- transaction (cashflow) FX rate to EUR
		left outer join PerfTest.FXRate fxt on fxt.Currency = 'EUR' and fxt.PriceCurrency = tv.TransCurrency and fxt.PriceDate = tv.ToDate and tv.TransCurrency <> 'EUR'
		-- instrument FX rate to EUR
		left outer join PerfTest.FXRate fxi on fxi.Currency = 'EUR' and fxi.PriceCurrency = tv.InstrumentCurrency and fxi.PriceDate = tv.ToDate and tv.InstrumentCurrency <> 'EUR'
		-- portfolio FX rate to EUR
		left outer join PerfTest.FXRate fxpor on fxpor.Currency = 'EUR' and fxpor.PriceCurrency = por.Currency and fxpor.PriceDate = tv.ToDate and por.Currency <> 'EUR'
		cross apply (
			-- using cross apply to simplify fx rate handling, as for base currency EUR there are no quotes in data
			select
				case when tv.PriceCurrency       = 'EUR' then 1.00000000 else fxp.FXRate   end as FXRatePrice,
				case when tv.TransCurrency       = 'EUR' then 1.00000000 else fxt.FXRate   end as FXRateTrans,  -- when tv.TransCurrency is null then 1.00000000
				case when tv.InstrumentCurrency  = 'EUR' then 1.00000000 else fxi.FXRate   end as FXRateInstrument,
				case when por.Currency           = 'EUR' then 1.00000000 else fxpor.FXRate end as FXRatePortfolio
		) fx
		cross apply (
			-- using cross apply to generate two rows for each data row from trans_valuations, one for Instrument Currency and one for Portfolio Currency, with respective FXRates.
			-- other currencies like ReportingCurrency or SystemBaseCurrency could be added, if required.
			select
				ccy_data.CalculationCurrencyType,
				ccy_data.CalculationCurrency,
				ccy_data.FXRateTrans,
				ccy_data.FXRatePrice
			from (
				values
					('InstrumentCurrency', tv.InstrumentCurrency, fx.FXRateTrans / fx.FXRateInstrument, fx.FXRatePrice / fx.FXRateInstrument),
					('PortfolioCurrency',  por.Currency,          fx.FXRateTrans / fx.FXRatePortfolio,  fx.FXRatePrice / fx.FXRatePortfolio )
				) ccy_data(CalculationCurrencyType, CalculationCurrency, FXRateTrans, FXRatePrice)
		) ccy
	group by
		tv.PortfolioIK,
		por.ShortName,
		por.LongName,
		por.Currency,
		tv.InstrumentIK,
		tv.ShortName,
		tv.LongName,
		tv.InstrumentCurrency,
		tv.ToDate,
		tv.PriceDate,
		tv.PriceCurrency,
		tv.Price,
		ccy.CalculationCurrencyType,
		ccy.CalculationCurrency,
		ccy.FXRateTrans,
		ccy.FXRatePrice
	),

-- add cumulative units since start and cash flow adjustments since last valuation, and calculate market values.
cum_data as (
	select
		d.PortfolioIK,
		d.PortfolioShortName,
		d.PortfolioLongName,
		d.PortfolioCurrency,
		d.InstrumentIK,
		d.ShortName,
		d.LongName,
		d.InstrumentCurrency,
		d.ToDate,
		d.PriceDate,
		d.PriceCurrency,
		d.TransPrice,
		d.Units,
		d.CalculationCurrencyType,
		d.CalculationCurrency,
		convert(numeric(19,8), d.FXRateTrans) as FXRateTrans,
		convert(numeric(19,8), d.FXRatePrice) as FXRatePrice,
		convert(numeric(19,6), d.Price) as Price,
		convert(numeric(19,2), d.CashFlow) as CashFlow,
		sum(d.Units) over (partition by d.PortfolioIK, d.InstrumentIK, d.CalculationCurrencyType order by d.ToDate rows between unbounded preceding and current row) as CumUnits,
		convert(numeric(19,2), sum(d.Units) over (partition by d.PortfolioIK, d.InstrumentIK, d.CalculationCurrencyType order by d.ToDate rows between unbounded preceding and current row) * d.Price) as MarketValue,
		convert(numeric(19,2), sum(cf.CashFlowAdjustment) over (partition by d.PortfolioIK, d.InstrumentIK, d.PriceDate, d.CalculationCurrencyType order by d.ToDate rows between unbounded preceding and current row)) as CashFlowAdjustment,
		convert(numeric(19,2), sum(d.CapitalCall) over (partition by d.PortfolioIK, d.InstrumentIK, d.CalculationCurrencyType order by d.ToDate rows between unbounded preceding and current row)) as Cum_CapitalCall,
		convert(numeric(19,2), sum(d.Distributions) over (partition by d.PortfolioIK, d.InstrumentIK, d.CalculationCurrencyType order by d.ToDate rows between unbounded preceding and current row)) as Cum_Distributions
	from
		dates d
		cross apply (
			-- use cash flow adjustment only after price date, as on price date it is already affecting the price.
			select
				case
					when d.PriceDate < d.ToDate then d.CashFlow 
					else 0.00 
				end as CashFlowAdjustment
		) cf
	)

-- finally, calculate DPI, RVPI, and TVPI, and filter out any possible price rows where fund not owned.
select 
	*,
	try_convert(numeric(19,4), convert(float, Cum_Distributions) / nullif(Cum_CapitalCall, 0)) as DPI,
	try_convert(numeric(19,4), convert(float, MarketValue + CashFlowAdjustment) / nullif(Cum_CapitalCall, 0)) as RVPI,
	try_convert(numeric(19,4), convert(float, MarketValue + CashFlowAdjustment + Cum_Distributions) / nullif(Cum_CapitalCall, 0)) as TVPI
from 
	cum_data 
where
	CumUnits > 0
	or Units < 0;  -- include possible final "sell" when CumUnits drops to zero.


