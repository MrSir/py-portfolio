document.getElementById('date').innerHTML = summary_data.date;
document.getElementById('username').innerHTML = summary_data.username;
document.getElementById('portfolio').innerHTML = summary_data.portfolio;
document.getElementById('currency').innerHTML = summary_data.currency;
document.getElementById('investedValue').innerHTML = "$" + summary_data.invested
document.getElementById('currentValue').innerHTML = "$" + summary_data.value
document.getElementById('numberOfEquities').innerHTML = summary_data.equities;
document.getElementById('numberOfETFs').innerHTML = summary_data.etfs;

var currentPercent = document.getElementById('currentPercent');
var percent_html = ""
if (summary_data.percent > 0) {
    currentPercent.classList.add("text-success");
    percent_html = '<i class="bi-arrow-up-short"></i>' + summary_data.percent + "%"
} else {
    currentPercent.classList.add("text-danger");
    percent_html = '<i class="bi-arrow-down-short"></i>' + summary_data.percent + "%"
}
currentPercent.innerHTML = percent_html;

var portfolioDataElement = document.getElementById('portfolioData');
var portfolio_data_html = ""
portfolio_data.forEach(function(stock) {
    invested = stock.invested.toFixed(2)
    value = stock.value.toFixed(2)

    value_class = ""
    if (stock.value > stock.invested) {
        value_class = "text-success"
    } else if (stock.value < stock.invested) {
        value_class = "text-danger"
    }

    portfolio_data_html = portfolio_data_html + '<tr>'
        + '<td class="col-3 text-center">' + stock.moniker + '</td>'
        + '<td class="col-2 text-end">$' + invested + '</td>'
        + '<td class="col-2 text-end ' + value_class + '">$' + value + '</td></tr>';
});
portfolioDataElement.innerHTML = portfolio_data_html


var sharesDataElement = document.getElementById('sharesData');
var shares_data_html = ""
portfolio_data.forEach(function(stock) {
    avg_price = stock.average_price.toFixed(2)
    mrkt_price = stock.market_price.toFixed(2)

    mrkt_price_class = ""
    if (stock.market_price > stock.average_price) {
        mrkt_price_class = "text-success"
    } else if (stock.market_price < stock.average_price) {
        mrkt_price_class = "text-danger"
    }

    shares_data_html = shares_data_html + '<tr>'
        + '<td class="col-3 text-center">' + stock.moniker + '</td>'
        + '<td class="col-1 text-end">' + stock.amount.toFixed(3) + '</td>'
        + '<td class="col-2 text-end">$' + avg_price + '</td>'
        + '<td class="col-2 text-end ' + mrkt_price_class + '">$' + mrkt_price + '</td></tr>';
});
sharesDataElement.innerHTML = shares_data_html
