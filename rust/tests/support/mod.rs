use std::process::{Command, Output};

pub struct TestServices {
    pub postgres: sqlx::PgPool,
    pub jetstream: async_nats::jetstream::Context,
    pub redis: redis::aio::MultiplexedConnection,
    pub namespace: String,
    pub workspace_id: uuid::Uuid,
    _stack: ComposeStack,
}

impl TestServices {
    pub async fn start() -> Self {
        let namespace = format!("relayhub_rust_test_{}", uuid::Uuid::new_v4().simple());
        let stack = ComposeStack(namespace.clone());
        stack.run(&["up", "-d", "--wait", "--wait-timeout", "60"]);
        let postgres = sqlx::PgPool::connect(&format!(
            "postgresql://relayhub_test@{}/relayhub_rust_test?sslmode=disable",
            stack.address("postgres", "5432")
        ))
        .await
        .unwrap();
        let nats = async_nats::connect(format!("nats://{}", stack.address("nats", "4222")))
            .await
            .unwrap();
        let redis = redis::Client::open(format!("redis://{}/0", stack.address("redis", "6379")))
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();
        Self {
            postgres,
            jetstream: async_nats::jetstream::new(nats),
            redis,
            namespace,
            workspace_id: uuid::Uuid::new_v4(),
            _stack: stack,
        }
    }
}

struct ComposeStack(String);

impl ComposeStack {
    fn command(&self) -> Command {
        let mut command = Command::new("docker");
        command.args([
            "compose",
            "--env-file",
            "/dev/null",
            "-f",
            concat!(env!("CARGO_MANIFEST_DIR"), "/tests/compose.yaml"),
            "-p",
            &self.0,
        ]);
        command
    }

    fn run(&self, args: &[&str]) -> Output {
        let output = self
            .command()
            .args(args)
            .output()
            .expect("Docker Compose is required");
        assert!(
            output.status.success(),
            "Compose {args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        output
    }

    fn address(&self, service: &str, port: &str) -> String {
        let output = self.run(&["port", service, port]);
        let address = String::from_utf8(output.stdout).unwrap().trim().to_owned();
        assert!(
            address.starts_with("127.0.0.1:"),
            "test service must be loopback only"
        );
        address
    }
}

impl Drop for ComposeStack {
    fn drop(&mut self) {
        match self
            .command()
            .args(["down", "--volumes", "--remove-orphans", "--timeout", "1"])
            .output()
        {
            Ok(output) if output.status.success() => {}
            result => eprintln!("test stack {} cleanup failed: {result:?}", self.0),
        }
    }
}
