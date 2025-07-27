-- Create BacktestRuns table
CREATE TABLE BacktestRuns (
    Id NVARCHAR(50) PRIMARY KEY,
    Name NVARCHAR(200) NOT NULL,
    FromDate DATETIME NOT NULL,
    ToDate DATETIME NOT NULL,
    InitialCapital DECIMAL(18, 2) NOT NULL,
    LotSize INT NOT NULL,
    SignalsToTest NVARCHAR(200) NOT NULL,
    UseHedging BIT NOT NULL DEFAULT 1,
    HedgeOffset INT NOT NULL DEFAULT 500,
    CommissionPerLot DECIMAL(18, 2) NOT NULL DEFAULT 40,
    SlippagePercent DECIMAL(5, 4) NOT NULL DEFAULT 0.001,
    Status NVARCHAR(20) NOT NULL,
    StartedAt DATETIME NULL,
    CompletedAt DATETIME NULL,
    ErrorMessage NVARCHAR(MAX) NULL,
    TotalTrades INT NOT NULL DEFAULT 0,
    WinningTrades INT NOT NULL DEFAULT 0,
    LosingTrades INT NOT NULL DEFAULT 0,
    WinRate DECIMAL(5, 2) NULL,
    FinalCapital DECIMAL(18, 2) NULL,
    TotalPnL DECIMAL(18, 2) NULL,
    TotalReturnPercent DECIMAL(8, 2) NULL,
    MaxDrawdown DECIMAL(18, 2) NULL,
    MaxDrawdownPercent DECIMAL(8, 2) NULL,
    CreatedAt DATETIME NOT NULL DEFAULT GETDATE(),
    CreatedBy NVARCHAR(100) NOT NULL DEFAULT 'System'
);

-- Create BacktestTrades table
CREATE TABLE BacktestTrades (
    Id NVARCHAR(50) PRIMARY KEY,
    BacktestRunId NVARCHAR(50) NOT NULL,
    WeekStartDate DATETIME NOT NULL,
    SignalType NVARCHAR(10) NOT NULL,
    Direction NVARCHAR(20) NOT NULL,
    EntryTime DATETIME NOT NULL,
    IndexPriceAtEntry DECIMAL(18, 2) NOT NULL,
    SignalTriggerPrice DECIMAL(18, 2) NOT NULL,
    StopLossPrice DECIMAL(18, 2) NOT NULL,
    ExitTime DATETIME NULL,
    IndexPriceAtExit DECIMAL(18, 2) NULL,
    Outcome NVARCHAR(20) NOT NULL,
    ExitReason NVARCHAR(200) NULL,
    TotalPnL DECIMAL(18, 2) NULL,
    FOREIGN KEY (BacktestRunId) REFERENCES BacktestRuns(Id) ON DELETE CASCADE
);

-- Create BacktestPositions table
CREATE TABLE BacktestPositions (
    Id NVARCHAR(50) PRIMARY KEY,
    TradeId NVARCHAR(50) NOT NULL,
    PositionType NVARCHAR(20) NOT NULL,
    OptionType NVARCHAR(2) NOT NULL,
    StrikePrice INT NOT NULL,
    ExpiryDate DATETIME NOT NULL,
    EntryTime DATETIME NOT NULL,
    EntryPrice DECIMAL(18, 2) NOT NULL,
    Quantity INT NOT NULL,
    ExitTime DATETIME NULL,
    ExitPrice DECIMAL(18, 2) NULL,
    GrossPnL DECIMAL(18, 2) NULL,
    Commission DECIMAL(18, 2) NULL,
    NetPnL DECIMAL(18, 2) NULL,
    FOREIGN KEY (TradeId) REFERENCES BacktestTrades(Id) ON DELETE CASCADE
);

-- Create BacktestDailyResults table
CREATE TABLE BacktestDailyResults (
    Id NVARCHAR(50) PRIMARY KEY,
    BacktestRunId NVARCHAR(50) NOT NULL,
    Date DATE NOT NULL,
    StartingCapital DECIMAL(18, 2) NOT NULL,
    EndingCapital DECIMAL(18, 2) NOT NULL,
    DailyPnL DECIMAL(18, 2) NOT NULL,
    DailyReturnPercent DECIMAL(8, 2) NOT NULL,
    TradesOpened INT NOT NULL DEFAULT 0,
    TradesClosed INT NOT NULL DEFAULT 0,
    OpenPositions INT NOT NULL DEFAULT 0,
    FOREIGN KEY (BacktestRunId) REFERENCES BacktestRuns(Id) ON DELETE CASCADE,
    UNIQUE (BacktestRunId, Date)
);

-- Create indexes for performance
CREATE INDEX IX_BacktestTrades_BacktestRunId ON BacktestTrades(BacktestRunId);
CREATE INDEX IX_BacktestTrades_SignalType ON BacktestTrades(SignalType);
CREATE INDEX IX_BacktestPositions_TradeId ON BacktestPositions(TradeId);
CREATE INDEX IX_BacktestDailyResults_BacktestRunId ON BacktestDailyResults(BacktestRunId);