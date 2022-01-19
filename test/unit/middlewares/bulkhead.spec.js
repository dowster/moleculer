const _ = require("lodash");
const ServiceBroker = require("../../../src/service-broker");
const Context = require("../../../src/context");
const Middleware = require("../../../src/middlewares").Bulkhead;
const { protectReject } = require("../utils");

describe("Test BulkheadMiddleware", () => {
	const broker = new ServiceBroker({ nodeID: "server-1", logger: false });
	const handler = jest.fn(() => Promise.resolve("Result"));
	const service = { fullName: "posts" };
	const action = {
		name: "posts.find",
		bulkhead: {
			enabled: false
		},
		handler,
		service
	};
	const event = {
		name: "user.created",
		bulkhead: {
			enabled: false
		},
		handler,
		service
	};
	const endpoint = {
		action,
		event,
		node: {
			id: broker.nodeID
		}
	};

	jest.spyOn(broker.metrics, "increment");
	jest.spyOn(broker.metrics, "decrement");
	jest.spyOn(broker.metrics, "set");

	const mw = Middleware(broker);
	mw.created(broker);

	it("should register hooks", () => {
		expect(mw.localAction).toBeInstanceOf(Function);
		expect(mw.localEvent).toBeInstanceOf(Function);
	});

	it("should not wrap handler if bulkhead is disabled", () => {
		broker.options.bulkhead.enabled = false;

		const newActionHandler = mw.localAction.call(broker, handler, action);
		expect(newActionHandler).toBe(handler);

		const newEventHandler = mw.localEvent.call(broker, handler, action);
		expect(newEventHandler).toBe(handler);
	});

	it("should register metrics", () => {
		broker.isMetricsEnabled = jest.fn(() => true);
		broker.metrics.register = jest.fn();

		mw.created.call(broker, broker);

		expect(broker.metrics.register).toHaveBeenCalledTimes(8);
		expect(broker.metrics.register).toHaveBeenCalledWith({
			type: "gauge",
			name: "moleculer.request.bulkhead.inflight",
			labelNames: ["action", "service"]
		});
		expect(broker.metrics.register).toHaveBeenCalledWith({
			type: "gauge",
			name: "moleculer.request.bulkhead.inflight.limit",
			labelNames: ["action", "service"]
		});
		expect(broker.metrics.register).toHaveBeenCalledWith({
			type: "gauge",
			name: "moleculer.request.bulkhead.queue.size",
			labelNames: ["action", "service"]
		});
		expect(broker.metrics.register).toHaveBeenCalledWith({
			type: "gauge",
			name: "moleculer.request.bulkhead.queue.size.limit",
			labelNames: ["action", "service"]
		});
		expect(broker.metrics.register).toHaveBeenCalledWith({
			type: "gauge",
			name: "moleculer.event.bulkhead.inflight",
			labelNames: ["event", "service"]
		});
		expect(broker.metrics.register).toHaveBeenCalledWith({
			type: "gauge",
			name: "moleculer.event.bulkhead.inflight.limit",
			labelNames: ["event", "service"]
		});
		expect(broker.metrics.register).toHaveBeenCalledWith({
			type: "gauge",
			name: "moleculer.event.bulkhead.queue.size",
			labelNames: ["event", "service"]
		});
		expect(broker.metrics.register).toHaveBeenCalledWith({
			type: "gauge",
			name: "moleculer.event.bulkhead.queue.size.limit",
			labelNames: ["event", "service"]
		});
	});

	describe("Test localAction", () => {
		it("should wrap handler if bulkhead is disabled but in action is enabled", () => {
			broker.options.bulkhead.enabled = false;
			action.bulkhead.enabled = true;

			const newHandler = mw.localAction.call(broker, handler, action);
			expect(newHandler).not.toBe(handler);
		});

		it("should wrap handler if bulkhead is enabled", () => {
			broker.options.bulkhead.enabled = true;

			const newHandler = mw.localAction.call(broker, handler, action);
			expect(newHandler).not.toBe(handler);
		});

		it("should not wrap handler if bulkhead is enabled but in action is disabled", () => {
			broker.options.bulkhead.enabled = true;
			action.bulkhead.enabled = false;

			const newHandler = mw.localAction.call(broker, handler, action);
			expect(newHandler).toBe(handler);
		});

		it("should call 3 times immediately & 7 times after some delay (queueing)", () => {
			action.bulkhead.enabled = true;
			action.bulkhead.concurrency = 3;
			action.bulkhead.maxQueueSize = 10;

			let FLOW = [];

			const handler = jest.fn(ctx => {
				FLOW.push("handler-" + ctx.params.id);
				return broker.Promise.delay(10 * ctx.params.id);
			});

			const newHandler = mw.localAction.call(broker, handler, action);

			const ctxs = _.times(10, i => Context.create(broker, endpoint, { id: i + 1 }));
			Promise.all(ctxs.map(ctx => newHandler.call(broker, ctx)));

			expect(FLOW).toEqual(expect.arrayContaining(["handler-1", "handler-2", "handler-3"]));
			expect(handler).toHaveBeenCalledTimes(3);

			expect(broker.metrics.set).toHaveBeenCalledTimes(40);

			const tags = { action: "posts.find", service: "posts" };

			const calls = [
				{ metric: "moleculer.request.bulkhead.inflight", value: 1 },
				{ metric: "moleculer.request.bulkhead.inflight.limit", value: 3 },
				{ metric: "moleculer.request.bulkhead.queue.size", value: 0 },
				{ metric: "moleculer.request.bulkhead.queue.size.limit", value: 10 },
				{ metric: "moleculer.request.bulkhead.inflight", value: 2 },
				{ metric: "moleculer.request.bulkhead.inflight.limit", value: 3 },
				{ metric: "moleculer.request.bulkhead.queue.size", value: 0 },
				{ metric: "moleculer.request.bulkhead.queue.size.limit", value: 10 },
				{ metric: "moleculer.request.bulkhead.inflight", value: 3 },
				{ metric: "moleculer.request.bulkhead.inflight.limit", value: 3 },
				{ metric: "moleculer.request.bulkhead.queue.size", value: 0 },
				{ metric: "moleculer.request.bulkhead.queue.size.limit", value: 10 },
				{ metric: "moleculer.request.bulkhead.inflight", value: 3 },
				{ metric: "moleculer.request.bulkhead.inflight.limit", value: 3 },
				{ metric: "moleculer.request.bulkhead.queue.size", value: 1 },
				{ metric: "moleculer.request.bulkhead.queue.size.limit", value: 10 },
				{ metric: "moleculer.request.bulkhead.inflight", value: 3 },
				{ metric: "moleculer.request.bulkhead.inflight.limit", value: 3 },
				{ metric: "moleculer.request.bulkhead.queue.size", value: 2 },
				{ metric: "moleculer.request.bulkhead.queue.size.limit", value: 10 },
				{ metric: "moleculer.request.bulkhead.inflight", value: 3 },
				{ metric: "moleculer.request.bulkhead.inflight.limit", value: 3 },
				{ metric: "moleculer.request.bulkhead.queue.size", value: 3 },
				{ metric: "moleculer.request.bulkhead.queue.size.limit", value: 10 },
				{ metric: "moleculer.request.bulkhead.inflight", value: 3 },
				{ metric: "moleculer.request.bulkhead.inflight.limit", value: 3 },
				{ metric: "moleculer.request.bulkhead.queue.size", value: 4 },
				{ metric: "moleculer.request.bulkhead.queue.size.limit", value: 10 },
				{ metric: "moleculer.request.bulkhead.inflight", value: 3 },
				{ metric: "moleculer.request.bulkhead.inflight.limit", value: 3 },
				{ metric: "moleculer.request.bulkhead.queue.size", value: 5 },
				{ metric: "moleculer.request.bulkhead.queue.size.limit", value: 10 },
				{ metric: "moleculer.request.bulkhead.inflight", value: 3 },
				{ metric: "moleculer.request.bulkhead.inflight.limit", value: 3 },
				{ metric: "moleculer.request.bulkhead.queue.size", value: 6 },
				{ metric: "moleculer.request.bulkhead.queue.size.limit", value: 10 },
				{ metric: "moleculer.request.bulkhead.inflight", value: 3 },
				{ metric: "moleculer.request.bulkhead.inflight.limit", value: 3 },
				{ metric: "moleculer.request.bulkhead.queue.size", value: 7 },
				{ metric: "moleculer.request.bulkhead.queue.size.limit", value: 10 }
			];

			for (const callNum in calls) {
				expect(broker.metrics.set).toHaveBeenNthCalledWith(
					Number(callNum) + 1,
					calls[callNum].metric,
					calls[callNum].value,
					tags
				);
			}
			broker.metrics.set.mockClear();

			FLOW = [];

			return broker.Promise.delay(500)
				.catch(protectReject)
				.then(() => {
					expect(FLOW).toEqual(
						expect.arrayContaining([
							"handler-4",
							"handler-5",
							"handler-6",
							"handler-7",
							"handler-8",
							"handler-9",
							"handler-10"
						])
					);
					expect(handler).toHaveBeenCalledTimes(10);

					expect(broker.metrics.set).toHaveBeenCalledTimes(68);
				});
		});

		it("should call 3 times immediately & 5 times after some delay (queueing) and throw error because 2 are out of queue size", () => {
			action.bulkhead.enabled = true;
			action.bulkhead.concurrency = 3;
			action.bulkhead.maxQueueSize = 5;

			let FLOW = [];

			const handler = jest.fn(ctx => {
				FLOW.push("handler-" + ctx.params.id);
				return broker.Promise.delay(10 * ctx.params.id);
			});

			const newHandler = mw.localAction.call(broker, handler, action);

			const ctxs = _.times(10, i => Context.create(broker, endpoint, { id: i + 1 }));
			const p = broker.Promise.all(
				ctxs.map(ctx =>
					newHandler
						.call(broker, ctx)
						.catch(err => FLOW.push(err.name + "-" + ctx.params.id))
				)
			);

			expect(FLOW).toEqual(expect.arrayContaining(["handler-1", "handler-2", "handler-3"]));
			expect(handler).toHaveBeenCalledTimes(3);

			FLOW = [];

			return p
				.delay(500)
				.catch(protectReject)
				.then(() => {
					expect(FLOW).toEqual(
						expect.arrayContaining([
							"handler-4",
							"QueueIsFullError-9",
							"QueueIsFullError-10",
							"handler-5",
							"handler-6",
							"handler-7",
							"handler-8"
						])
					);
					expect(handler).toHaveBeenCalledTimes(8);
				});
		});

		it("should call 3 times immediately & 7 times after some delay (queueing) and some request rejected", () => {
			action.bulkhead.enabled = true;
			action.bulkhead.concurrency = 3;
			action.bulkhead.maxQueueSize = 10;
			broker.metrics.set.mockClear();

			let FLOW = [];

			const handler = jest.fn(ctx => {
				FLOW.push("handler-" + ctx.params.id);
				if (ctx.params.crash)
					return broker.Promise.reject(new Error("Crashed")).delay(10 * ctx.params.id);
				else return broker.Promise.delay(10 * ctx.params.id);
			});

			const newHandler = mw.localAction.call(broker, handler, action);

			const ctxs = _.times(10, i =>
				Context.create(broker, endpoint, { id: i + 1, crash: i == 1 || i == 7 })
			);
			const p = broker.Promise.all(
				ctxs.map(ctx =>
					newHandler
						.call(broker, ctx)
						.catch(err => FLOW.push(err.name + "-" + ctx.params.id))
				)
			);

			expect(FLOW).toEqual(expect.arrayContaining(["handler-1", "handler-2", "handler-3"]));
			expect(handler).toHaveBeenCalledTimes(3);
			expect(broker.metrics.set).toHaveBeenCalledTimes(40);

			broker.metrics.set.mockClear();

			FLOW = [];

			return p
				.delay(500)
				.catch(protectReject)
				.then(() => {
					expect(FLOW).toEqual(
						expect.arrayContaining([
							"handler-4",
							"Error-2",
							"handler-5",
							"handler-6",
							"handler-7",
							"handler-8",
							"handler-9",
							"Error-8",
							"handler-10"
						])
					);
					expect(handler).toHaveBeenCalledTimes(10);

					expect(broker.metrics.set).toHaveBeenCalledTimes(68);
				});
		});
	});

	describe("Test localEvent", () => {
		it("should wrap handler if bulkhead is disabled but in event is enabled", () => {
			broker.options.bulkhead.enabled = false;
			event.bulkhead.enabled = true;

			const newHandler = mw.localEvent.call(broker, handler, event);
			expect(newHandler).not.toBe(handler);
		});

		it("should wrap handler if bulkhead is enabled", () => {
			broker.options.bulkhead.enabled = true;

			const newHandler = mw.localEvent.call(broker, handler, event);
			expect(newHandler).not.toBe(handler);
		});

		it("should not wrap handler if bulkhead is enabled but in event is disabled", () => {
			broker.options.bulkhead.enabled = true;
			event.bulkhead.enabled = false;

			const newHandler = mw.localEvent.call(broker, handler, event);
			expect(newHandler).toBe(handler);
		});

		it("should call 3 times immediately & 7 times after some delay (queueing)", () => {
			event.bulkhead.enabled = true;
			event.bulkhead.concurrency = 3;
			event.bulkhead.maxQueueSize = 10;

			broker.metrics.set.mockClear();

			let FLOW = [];

			const handler = jest.fn(ctx => {
				FLOW.push("handler-" + ctx.params.id);
				return broker.Promise.delay(10 * ctx.params.id);
			});

			const newHandler = mw.localEvent.call(broker, handler, event);

			const ctxs = _.times(10, i => Context.create(broker, endpoint, { id: i + 1 }));
			Promise.all(ctxs.map(ctx => newHandler.call(broker, ctx)));

			expect(FLOW).toEqual(expect.arrayContaining(["handler-1", "handler-2", "handler-3"]));
			expect(handler).toHaveBeenCalledTimes(3);

			expect(broker.metrics.set).toHaveBeenCalledTimes(40);
			const tags = { event: "user.created", service: "posts" };

			const calls = [
				{ metric: "moleculer.event.bulkhead.inflight", value: 1 },
				{ metric: "moleculer.event.bulkhead.inflight.limit", value: 3 },
				{ metric: "moleculer.event.bulkhead.queue.size", value: 0 },
				{ metric: "moleculer.event.bulkhead.queue.size.limit", value: 10 },
				{ metric: "moleculer.event.bulkhead.inflight", value: 2 },
				{ metric: "moleculer.event.bulkhead.inflight.limit", value: 3 },
				{ metric: "moleculer.event.bulkhead.queue.size", value: 0 },
				{ metric: "moleculer.event.bulkhead.queue.size.limit", value: 10 },
				{ metric: "moleculer.event.bulkhead.inflight", value: 3 },
				{ metric: "moleculer.event.bulkhead.inflight.limit", value: 3 },
				{ metric: "moleculer.event.bulkhead.queue.size", value: 0 },
				{ metric: "moleculer.event.bulkhead.queue.size.limit", value: 10 },
				{ metric: "moleculer.event.bulkhead.inflight", value: 3 },
				{ metric: "moleculer.event.bulkhead.inflight.limit", value: 3 },
				{ metric: "moleculer.event.bulkhead.queue.size", value: 1 },
				{ metric: "moleculer.event.bulkhead.queue.size.limit", value: 10 },
				{ metric: "moleculer.event.bulkhead.inflight", value: 3 },
				{ metric: "moleculer.event.bulkhead.inflight.limit", value: 3 },
				{ metric: "moleculer.event.bulkhead.queue.size", value: 2 },
				{ metric: "moleculer.event.bulkhead.queue.size.limit", value: 10 },
				{ metric: "moleculer.event.bulkhead.inflight", value: 3 },
				{ metric: "moleculer.event.bulkhead.inflight.limit", value: 3 },
				{ metric: "moleculer.event.bulkhead.queue.size", value: 3 },
				{ metric: "moleculer.event.bulkhead.queue.size.limit", value: 10 },
				{ metric: "moleculer.event.bulkhead.inflight", value: 3 },
				{ metric: "moleculer.event.bulkhead.inflight.limit", value: 3 },
				{ metric: "moleculer.event.bulkhead.queue.size", value: 4 },
				{ metric: "moleculer.event.bulkhead.queue.size.limit", value: 10 },
				{ metric: "moleculer.event.bulkhead.inflight", value: 3 },
				{ metric: "moleculer.event.bulkhead.inflight.limit", value: 3 },
				{ metric: "moleculer.event.bulkhead.queue.size", value: 5 },
				{ metric: "moleculer.event.bulkhead.queue.size.limit", value: 10 },
				{ metric: "moleculer.event.bulkhead.inflight", value: 3 },
				{ metric: "moleculer.event.bulkhead.inflight.limit", value: 3 },
				{ metric: "moleculer.event.bulkhead.queue.size", value: 6 },
				{ metric: "moleculer.event.bulkhead.queue.size.limit", value: 10 },
				{ metric: "moleculer.event.bulkhead.inflight", value: 3 },
				{ metric: "moleculer.event.bulkhead.inflight.limit", value: 3 },
				{ metric: "moleculer.event.bulkhead.queue.size", value: 7 },
				{ metric: "moleculer.event.bulkhead.queue.size.limit", value: 10 }
			];

			for (const callNum in calls) {
				expect(broker.metrics.set).toHaveBeenNthCalledWith(
					Number(callNum) + 1,
					calls[callNum].metric,
					calls[callNum].value,
					tags
				);
			}

			broker.metrics.set.mockClear();

			FLOW = [];

			return broker.Promise.delay(500)
				.catch(protectReject)
				.then(() => {
					expect(FLOW).toEqual(
						expect.arrayContaining([
							"handler-4",
							"handler-5",
							"handler-6",
							"handler-7",
							"handler-8",
							"handler-9",
							"handler-10"
						])
					);
					expect(handler).toHaveBeenCalledTimes(10);

					expect(broker.metrics.set).toHaveBeenCalledTimes(68);
				});
		});

		it("should call 3 times immediately & 5 times after some delay (queueing) and throw error because 2 are out of queue size", () => {
			event.bulkhead.enabled = true;
			event.bulkhead.concurrency = 3;
			event.bulkhead.maxQueueSize = 5;

			let FLOW = [];

			const handler = jest.fn(ctx => {
				FLOW.push("handler-" + ctx.params.id);
				return broker.Promise.delay(10 * ctx.params.id);
			});

			const newHandler = mw.localEvent.call(broker, handler, event);

			const ctxs = _.times(10, i => Context.create(broker, endpoint, { id: i + 1 }));
			const p = broker.Promise.all(
				ctxs.map(ctx =>
					newHandler
						.call(broker, ctx)
						.catch(err => FLOW.push(err.name + "-" + ctx.params.id))
				)
			);

			expect(FLOW).toEqual(expect.arrayContaining(["handler-1", "handler-2", "handler-3"]));
			expect(handler).toHaveBeenCalledTimes(3);

			FLOW = [];

			return p
				.delay(500)
				.catch(protectReject)
				.then(() => {
					expect(FLOW).toEqual(
						expect.arrayContaining([
							"handler-4",
							"QueueIsFullError-9",
							"QueueIsFullError-10",
							"handler-5",
							"handler-6",
							"handler-7",
							"handler-8"
						])
					);
					expect(handler).toHaveBeenCalledTimes(8);
				});
		});

		it("should call 3 times immediately & 7 times after some delay (queueing) and some request rejected", () => {
			event.bulkhead.enabled = true;
			event.bulkhead.concurrency = 3;
			event.bulkhead.maxQueueSize = 10;
			broker.metrics.set.mockClear();

			let FLOW = [];

			const handler = jest.fn(ctx => {
				FLOW.push("handler-" + ctx.params.id);
				if (ctx.params.crash)
					return broker.Promise.reject(new Error("Crashed")).delay(10 * ctx.params.id);
				else return broker.Promise.delay(10 * ctx.params.id);
			});

			const newHandler = mw.localEvent.call(broker, handler, event);

			const ctxs = _.times(10, i =>
				Context.create(broker, endpoint, { id: i + 1, crash: i == 1 || i == 7 })
			);
			const p = broker.Promise.all(
				ctxs.map(ctx =>
					newHandler
						.call(broker, ctx)
						.catch(err => FLOW.push(err.name + "-" + ctx.params.id))
				)
			);

			expect(FLOW).toEqual(expect.arrayContaining(["handler-1", "handler-2", "handler-3"]));
			expect(handler).toHaveBeenCalledTimes(3);
			expect(broker.metrics.set).toHaveBeenCalledTimes(40);

			broker.metrics.set.mockClear();

			FLOW = [];

			return p
				.delay(500)
				.catch(protectReject)
				.then(() => {
					expect(FLOW).toEqual(
						expect.arrayContaining([
							"handler-4",
							"Error-2",
							"handler-5",
							"handler-6",
							"handler-7",
							"handler-8",
							"handler-9",
							"Error-8",
							"handler-10"
						])
					);
					expect(handler).toHaveBeenCalledTimes(10);

					expect(broker.metrics.set).toHaveBeenCalledTimes(68);
				});
		});
	});
});
