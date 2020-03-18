const rp = require('request-promise');
const cheerio = require('cheerio');
const _ = require('lodash');
const math = require('mathjs');

const AWS = require('aws-sdk');

AWS.config.update({
  region: 'ap-northeast-1',
});

var docClient = new AWS.DynamoDB.DocumentClient();

AWS.config.update({ region: 'us-west-2' });
const ses = new AWS.SES({ apiVersion: '2010-12-01' });

const worker = async () => {
  let $ = cheerio.load(
    await rp({
      uri: 'https://stock.wespai.com/lists',
      headers: {
        'user-agent':
          'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.132 Safari/537.36',
      },
      json: true,
    }),
  );

  let parseDepositStocks = $('#example tbody tr')
    .map((index, stock) => {
      return {
        symbol: $(stock)
          .children('td')
          .eq(0)
          .text(),
        company:
          $(stock)
            .children('td')
            .eq(1)
            .text() ||
          $(stock)
            .children('td')
            .eq(0)
            .text(),
        price: parseFloat(
          $(stock)
            .children('td')
            .eq(3)
            .text(),
        ),
      };
    })
    .get();

  for (let stock of parseDepositStocks) {
    await new Promise(resolve => setTimeout(() => resolve(), 100));
    console.log(stock.symbol);
    $ = cheerio.load(
      await rp({
        uri: 'https://goodinfo.tw/StockInfo/StockDividendPolicy.asp',
        qs: {
          STOCK_ID: stock.symbol,
        },
        headers: {
          'user-agent':
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.132 Safari/537.36',
        },
        json: true,
      }),
    );
    stock.dividendCount = 0;
    stock.cashCount = 0;
    stock.rightCount = 0;
    stock.cashRecoveredCount = 0;
    stock.rightRecoveredCount = 0;
    stock.cashRecoveredRate = 0;
    stock.rightRecoveredRate = 0;
    stock.avgDividendYield = 0;
    stock.sumDividendYield = 0;

    const dividendList = _.reduce(
      $('#divDetail tbody tr'),
      (accu, item, index) => {
        const year = $(item)
          .children('td')
          .eq(0)
          .text();
        const dividend =
          $(item)
            .children('td')
            .eq(7)
            .text() !== '-'
            ? parseFloat(
                $(item)
                  .children('td')
                  .eq(7)
                  .text(),
              )
            : 0;
        const cashRecoveredDay =
          $(item)
            .children('td')
            .eq(10)
            .text() !== '-'
            ? parseFloat(
                $(item)
                  .children('td')
                  .eq(10)
                  .text(),
              )
            : 0;
        const rightRecoveredDay =
          $(item)
            .children('td')
            .eq(11)
            .text() !== '-'
            ? parseFloat(
                $(item)
                  .children('td')
                  .eq(11)
                  .text(),
              )
            : 0;
        const cashTotal =
          $(item)
            .children('td')
            .eq(3)
            .text() !== '-'
            ? parseFloat(
                $(item)
                  .children('td')
                  .eq(3)
                  .text(),
              )
            : 0;
        const rightTotal =
          $(item)
            .children('td')
            .eq(6)
            .text() !== '-'
            ? parseFloat(
                $(item)
                  .children('td')
                  .eq(6)
                  .text(),
              )
            : 0;
        const dividendYield =
          $(item)
            .children('td')
            .eq(18)
            .text() !== '-'
            ? parseFloat(
                $(item)
                  .children('td')
                  .eq(18)
                  .text(),
              )
            : 0;
        if (year === '累計') return accu;
        if (!(dividend > 0)) return accu;

        if (!(cashTotal > 0) && !(rightTotal > 0)) return accu;
        stock.dividendCount++;
        stock.sumDividendYield += dividendYield;

        if (cashTotal > 0) stock.cashCount++;
        if (rightTotal > 0) stock.rightCount++;
        if (cashRecoveredDay && cashRecoveredDay <= 366)
          stock.cashRecoveredCount++;
        if (rightRecoveredDay && rightRecoveredDay <= 366)
          stock.rightRecoveredCount++;

        accu.push({
          id: stock.symbol + '_' + index,
          symbol: stock.symbol,
          company: stock.company,
          year,
          cashSurplus:
            $(item)
              .children('td')
              .eq(1)
              .text() !== '-'
              ? parseFloat(
                  $(item)
                    .children('td')
                    .eq(1)
                    .text(),
                )
              : 0,
          cashAdditionalPaidIn:
            $(item)
              .children('td')
              .eq(2)
              .text() !== '-'
              ? parseFloat(
                  $(item)
                    .children('td')
                    .eq(2)
                    .text(),
                )
              : 0,
          cashTotal,
          rightSurplus:
            $(item)
              .children('td')
              .eq(4)
              .text() !== '-'
              ? parseFloat(
                  $(item)
                    .children('td')
                    .eq(4)
                    .text(),
                )
              : 0,
          rightAdditionalPaidIn:
            $(item)
              .children('td')
              .eq(5)
              .text() !== '-'
              ? parseFloat(
                  $(item)
                    .children('td')
                    .eq(5)
                    .text(),
                )
              : 0,
          rightTotal,
          dividend,
          cashB:
            $(item)
              .children('td')
              .eq(8)
              .text() !== '-'
              ? parseFloat(
                  $(item)
                    .children('td')
                    .eq(8)
                    .text(),
                )
              : 0,
          rightK:
            $(item)
              .children('td')
              .eq(9)
              .text() !== '-'
              ? parseFloat(
                  $(item)
                    .children('td')
                    .eq(9)
                    .text(),
                )
              : 0,
          cashRecoveredDay,
          rightRecoveredDay,
          year1: $(item)
            .children('td')
            .eq(12)
            .text(),
          maxPrice:
            $(item)
              .children('td')
              .eq(13)
              .text() !== '-'
              ? parseFloat(
                  $(item)
                    .children('td')
                    .eq(13)
                    .text(),
                )
              : 0,
          minPrice:
            $(item)
              .children('td')
              .eq(14)
              .text() !== '-'
              ? parseFloat(
                  $(item)
                    .children('td')
                    .eq(14)
                    .text(),
                )
              : 0,
          avgPrice:
            $(item)
              .children('td')
              .eq(15)
              .text() !== '-'
              ? parseFloat(
                  $(item)
                    .children('td')
                    .eq(15)
                    .text(),
                )
              : 0,
          cashDividendYield:
            $(item)
              .children('td')
              .eq(16)
              .text() !== '-'
              ? parseFloat(
                  $(item)
                    .children('td')
                    .eq(16)
                    .text(),
                )
              : 0,
          rightDividendYield:
            $(item)
              .children('td')
              .eq(17)
              .text() !== '-'
              ? parseFloat(
                  $(item)
                    .children('td')
                    .eq(17)
                    .text(),
                )
              : 0,
          dividendYield,
          year2: $(item)
            .children('td')
            .eq(19)
            .text(),
          eps:
            $(item)
              .children('td')
              .eq(20)
              .text() !== '-'
              ? parseFloat(
                  $(item)
                    .children('td')
                    .eq(20)
                    .text(),
                )
              : 0,
          cashDPR:
            $(item)
              .children('td')
              .eq(21)
              .text() !== '-'
              ? parseFloat(
                  $(item)
                    .children('td')
                    .eq(21)
                    .text(),
                )
              : 0,
          rightDPR:
            $(item)
              .children('td')
              .eq(22)
              .text() !== '-'
              ? parseFloat(
                  $(item)
                    .children('td')
                    .eq(22)
                    .text(),
                )
              : 0,
          dpr:
            $(item)
              .children('td')
              .eq(23)
              .text() !== '-'
              ? parseFloat(
                  $(item)
                    .children('td')
                    .eq(23)
                    .text(),
                )
              : 0,
        });
        return accu;
      },
      [],
    );

    if (_.isEmpty(dividendList)) continue;

    if (stock.cashCount > 0)
      stock.cashRecoveredRate = stock.cashRecoveredCount / stock.cashCount;
    if (stock.rightCount > 0)
      stock.rightRecoveredRate = stock.rightRecoveredCount / stock.rightCount;
    if (stock.dividendCount > 0)
      stock.avgDividendYield = stock.sumDividendYield / stock.dividendCount;
    stock.dividendList = dividendList;

    for (let i = 0; i <= parseInt(dividendList.length / 25); i++) {
      const list = dividendList.slice(i * 25, (i + 1) * 25);
      if (_.isEmpty(list)) break;

      await new Promise((resolve, reject) => {
        const now = new Date();
        docClient.batchWrite(
          {
            RequestItems: {
              dividendLists: list.map(o => ({
                PutRequest: {
                  Item: {
                    ...o,
                    created: now.getTime(),
                  },
                },
              })),
            },
          },
          function(err, data) {
            if (err) {
              console.error(
                'Unable to add item. Error JSON:',
                JSON.stringify(err, null, 2),
              );
              console.log(list);
              reject(err);
            } else {
              console.log('Added item:', JSON.stringify(data, null, 2));
              resolve(data);
            }
          },
        );
      });
    }
  }
};

exports.handler = async function(event, context) {
  await worker();
  return 'ok';
};
