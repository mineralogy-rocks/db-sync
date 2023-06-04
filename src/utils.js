

let trs = [...document.querySelectorAll('.results tr')].slice(1);
let data = [];
for (let tr of trs) {
    let _tds = tr.querySelectorAll('td');
    let _links = [..._tds[14].querySelectorAll('a')].map(_link => _link.getAttribute('href'));
    data.push({
        'mineral_name': _tds[1].innerText,
        'formula': _tds[2].innerHTML,
        'a': _tds[3].innerHTML,
        'b': _tds[4].innerHTML,
        'c': _tds[5].innerHTML,
        'alpha': _tds[6].innerHTML,
        'beta': _tds[7].innerHTML,
        'gamma': _tds[8].innerHTML,
        'volume': _tds[9].innerHTML,
        'space_group': _tds[11].innerHTML,
        'links': _links,
        'reference': _tds[13].innerText,
        'note': _tds[15].innerText
    })
}
