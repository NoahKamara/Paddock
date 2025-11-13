//
//  ImageTests.swift
//
//  Copyright © 2024 Noah Kamara.
//

import Logging
import NIO
@testable import Paddock
import XCTest

final class ImageTests: XCTestCase {
    var client: PaddockClient!

    override func setUp() async throws {
        self.client = PaddockClient.forTesting()
        if await (try? self.client.images.get("nginx:latest")) == nil {
            _ = try await self.client.images.pull(byName: "nginx", tag: "latest")
        }
    }

    override func tearDownWithError() throws {
        try self.client.syncShutdown()
    }

    func testDeleteImage() async throws {
        let deleted = try await client.images.remove("nginx:latest", force: true)

        let results = Set(deleted)
        for result in results {
            switch result {
            case .deleted(let value):
                XCTAssertEqual(
                    value,
                    "7a3f95c078122f959979fac556ae6f43746c9f32e5a66526bb503ed1d4adbd07"
                )
            case .untagged(let value):
                XCTAssertTrue(value.contains("nginx:latest"))
            }
        }
    }

    func testPullImage() async throws {
        let imageInfo = try await client.images.pull(byName: "nginx", tag: "latest")
        let image = try await client.images.get(imageInfo.digest)

        XCTAssertTrue(image.repoTags!.contains(where: { $0.contains("nginx:latest") }))
    }

    func testPushImage() async throws {
        let password = "P@sSw0rd"
        var credentials = RegistryAuth(
            username: "johnsmith",
            password: password,
            serverAddress: URL(string: "https://registry.gitlab.com")!
        )
        // just verifies that credentials are good, but you *can* skip over this step and just
        // `entoken()` your credentials directly
        let token = try await client.registries.login(
            credentials: &credentials,
            logger: self.client.logger
        )

        let tag = "29455605-C9F2-4C13-9E79-E8E97A96695C"
        let image = try await client.images.get("alpine:latest")
        // if using a non `hub.docker.com` repo, the name needs to include the service as seen here
        try await self.client.images.tag(
            image.id,
            repoName: "registry.gitlab.com/johnsmith/tests",
            tag: tag
        )

        try await self.client.images.push(
            "registry.gitlab.com/johnsmith/tests",
            tag: tag,
            token: token
        )
    }

    func testListImage() async throws {
        let images = try await client.images.list()

        XCTAssert(images.count >= 1)
    }

    /* func testParsingRepositoryTagSuccessfull() {
     let rt = Image.RepositoryTag("hello-world:latest")

     XCTAssertEqual(rt?.repository, "hello-world")
     XCTAssertEqual(rt?.tag, "latest")
     }

     func testParsingRepositoryTagThreeComponents() {
     let rt = Image.RepositoryTag("hello-world:latest:anotherone")

     XCTAssertNil(rt)
     }

     func testParsingRepositoryTagOnlyRepo() {
     let rt = Image.RepositoryTag("hello-world")

     XCTAssertEqual(rt?.repository, "hello-world")
     XCTAssertNil(rt?.tag)
     }

     func testParsingRepositoryTagWithDigest() {
     let rt = Image.RepositoryTag("sha256:89b647c604b2a436fc3aa56ab1ec515c26b085ac0c15b0d105bc475be15738fb")

     XCTAssertNil(rt)
     } */

    func testInspectImage() async throws {
        let image = try await client.images.get("nginx:latest")
        let repoTags = try XCTUnwrap(image.repoTags)
        XCTAssertTrue(
            repoTags.contains(where: { $0.contains("nginx:latest") }),
            "Ensure repoTags exists"
        )
    }

    func testImageHistory() async throws {
        let image = try await client.images.get("nginx:latest")
        let history = try await client.images.history(image.id)
        XCTAssert(history.count > 0 && history.first!.id.starts(with: "sha256"))
    }

    func testInspectImageWithoutContainerConfig() async throws {
        let image = try await client.images.get("traefik/whoami:latest")
        // Ensure decode succeeded even though ContainerConfig is absent
        XCTAssertNil(image.containerConfig)
        // Ensure Config is still decoded
        XCTAssertNotNil(image.config.entrypoint)
    }

    func testPruneImages() async throws {
        let image = try await client.images.get("nginx:latest")
        let cleanID = if self.client.state.hostInfo?.engine == .podman {
            String(image.id.trimmingPrefix("sha256:"))
        } else {
            image.id
        }
        let pruned = try await client.images.prune(all: true)
        XCTAssertTrue(pruned.imageIds.contains(cleanID))
        XCTAssertGreaterThan(pruned.reclaimedSpace, 0)
    }

    func testBuild() async throws {
        let tarPath = Bundle.module.url(
            forResource: "docker-context",
            withExtension: "tar",
            subdirectory: "Assets"
        )!

        let tar = try Data(contentsOf: tarPath)
        let buffer = ByteBuffer(data: tar)
        var imageID: String!
        do {
            let buildOutput = try await client.images.build(
                config: .init(
                    repoTags: ["build:test"],
                    buildArgs: ["TEST": "test"],
                    labels: ["test": "value"]
                ),
                context: buffer
            )

            for try await item in buildOutput {
                guard let auxID = item.aux?.id else { continue }
                imageID = auxID
            }
            XCTAssertNotNil(imageID, "Ensure built Image ID is returned")
            addTeardownBlock { [client, imageID] in
                try await client?.images.remove(imageID!)
            }
        } catch {
            print("\n•••• BOOM! \(error)")
            throw error
        }
        let image = try await client.images.get(imageID)
        let repoTags = try XCTUnwrap(image.repoTags)
        XCTAssertTrue(
            repoTags.contains(where: { $0.contains("build:test") }),
            "Ensure repo and tag are set"
        )
        let labels = try XCTUnwrap(image.config.labels)
        XCTAssertEqual(labels["test"], "value", "Ensure labels are set")
    }

    func testCommit() async throws {
        let container = try await client.containers
            .create(config: ContainerConfig(image: "nginx:latest"))
        try await self.client.containers.start(container.id)
        let imageID = try await client.images.commitFromContainer(
            named: container.id,
            repo: "test-commit",
            tag: "latest"
        )
        let image = try await client.images.get(imageID.id)
        let repoTags = try XCTUnwrap(image.repoTags)
        XCTAssertTrue(
            repoTags.contains(where: { $0.contains("test-commit:latest") }),
            "Ensure image has custom repo and tag"
        )
    }
}
