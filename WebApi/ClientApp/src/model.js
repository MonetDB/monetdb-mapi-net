var _listener;

class Model {
    host = "127.0.0.1"
    port = "50000"
    username = "monetdb"
    password = "monetdb"
    database = "demo"

    query

    meta
    data
    error
    estimate
    loading

    listen(fn) {
        _listener = fn;
    }

    execute(q, callback) {
        this.query = q;
        this.loading = true;

        var start = new Date();
        return fetch('api/SampleData/Execute', {
            method: 'POST',
            mode: 'cors',
            credentials: 'same-origin', // include, *same-origin, omit
            headers: {
                'Content-Type': 'application/json',
                // 'Content-Type': 'application/x-www-form-urlencoded',
            },
            body: JSON.stringify({
                host: this.host,
                port: parseInt(this.port),
                username: this.username,
                password: this.password,
                database: this.database,
                query: q
            })
        })
            .then(response => response.json())
            .then(x => {
                this.loading = false;
                this.estimate = (new Date() - start) / 1000;

                Object.assign(this, {
                    meta: null,
                    data: null,
                    error: null,
                    ...x
                });

                if (!callback || (callback(this) !== false)) {
                    _listener && _listener(this);
                }

                return this;
            });
    }

    reload() {
        this.execute(this.query);
    }
}

export default new Model();