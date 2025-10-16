mkdir css js

wget -q -O js/jquery-3.6.0.min.js \
  https://code.jquery.com/jquery-3.6.0.min.js

wget -q -O js/vis-network.min.js \
  https://unpkg.com/vis-network/standalone/umd/vis-network.min.js

wget -q -O js/tailwindcss-browser-4.js \
  https://cdn.jsdelivr.net/npm/@tailwindcss/browser@4

wget -q -O js/autoComplete.min.js \
  https://cdn.jsdelivr.net/npm/@tarekraafat/autocomplete.js@10.2.9/dist/autoComplete.min.js

wget -q -O js/popper-core-2.js \
  https://unpkg.com/@popperjs/core@2

wget -q -O js/tippy-6.js \
  https://unpkg.com/tippy.js@6

wget -q -O js/buttons.js \
  https://buttons.github.io/buttons.js

wget -q -O css/tippy.css \
  https://unpkg.com/tippy.js@6/dist/tippy.css

wget -O fa.zip https://use.fontawesome.com/releases/v7.1.0/fontawesome-free-7.1.0-web.zip?_gl=1*pfvczc*_ga*MTAzMzUyNDE2Ni4xNzYwNjA0NjUw*_ga_BPMS41FJD2*czE3NjA2MDQ2NDkkbzEkZzAkdDE3NjA2MDQ2NDkkajYwJGwwJGgw
unzip fa.zip
mv fontawesome-free-7.1.0-web fa
rm fa.zip